package filters

import (
	"bytes"
	"context"
	"errors"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/bloombits"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type TxFilter struct {
	sys *FilterSystem

	fromAddresses, toAddresses []common.Address
	sigHashes                  [][]byte

	// TODO filter transaction success

	block             *common.Hash // Block hash if filtering a single block
	begin, end, limit int64        // Range interval if filtering multiple blocks

	matcher *bloombits.Matcher
}

// NewTxRangeFilter creates a new filter which uses a bloom filter on blocks to
// figure out whether a particular block is interesting or not.
func (sys *FilterSystem) NewTxRangeFilter(begin, end, limit int64, fromAddresses, toAddresses []common.Address, sigHashes [][]byte) *TxFilter {

	var filters [][][]byte

	var addFilterAddresses = func(addresses []common.Address) {
		if len(addresses) > 0 {
			filter := make([][]byte, len(addresses))
			for i, address := range addresses {
				filter[i] = address.Bytes()
			}
			filters = append(filters, filter)
		}
	}

	addFilterAddresses(fromAddresses)
	addFilterAddresses(toAddresses)

	// SigHashes need no conversion
	filters = append(filters, sigHashes)

	size, _ := sys.backend.BloomStatus()

	filter := newTxFilter(sys, fromAddresses, toAddresses, sigHashes)

	filter.matcher = bloombits.NewMatcher(size, filters)
	filter.begin = begin
	filter.end = end
	filter.limit = limit

	return filter
}

// NewTxBlockFilter creates a new filter which directly inspects the contents of
// a block to figure out whether it is interesting or not.
func (sys *FilterSystem) NewTxBlockFilter(block common.Hash, fromAddresses, toAddresses []common.Address, sigHashes [][]byte) *TxFilter {
	filter := newTxFilter(sys, fromAddresses, toAddresses, sigHashes)
	filter.block = &block
	return filter
}

func newTxFilter(sys *FilterSystem, fromAddresses, toAddresses []common.Address, sigHashes [][]byte) *TxFilter {
	return &TxFilter{
		sys:           sys,
		fromAddresses: fromAddresses,
		toAddresses:   toAddresses,
		sigHashes:     sigHashes,
	}
}

// Transactions gets the matching transactions for the filter
func (f *TxFilter) Transactions(ctx context.Context) ([]*ethapi.RPCTransaction, error) {
	// If we're doing singleton block filtering, execute and return
	if f.block != nil {
		header, err := f.sys.backend.HeaderByHash(ctx, *f.block)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, errors.New("unknown block")
		}
		txs, err := f.blockTransactions(ctx, header)
		return txs, err
	}

	var (
		beginPending = f.begin == rpc.PendingBlockNumber.Int64()
		endPending   = f.end == rpc.PendingBlockNumber.Int64()
	)

	// special case for pending logs
	if beginPending && !endPending {
		return nil, errInvalidBlockRange
	}

	// Short-cut if all we care about is pending logs
	if beginPending && endPending {
		return f.pendingTransactions()
	}

	var err error
	// range query need to resolve the special begin/end block number
	if f.begin, err = resolveSpecial(f.sys, ctx, f.begin); err != nil {
		return nil, err
	}
	if f.end, err = resolveSpecial(f.sys, ctx, f.end); err != nil {
		return nil, err
	}

	var limitChan = make(chan bool, 1)
	defer close(limitChan)

	txChan, errChan := f.rangeTransactionsAsync(ctx, limitChan)
	var txs []*ethapi.RPCTransaction

	var checkLimit = func() bool {
		if f.limit != 0 && len(txs) >= int(f.limit) {
			limitChan <- true
			return true
		}
		return false
	}

	for {
		select {
		case tx := <-txChan:
			txs = append(txs, tx)
			if checkLimit() {
				return txs, nil
			}
		case err := <-errChan:
			if err != nil {
				// if an error occurs during extraction, we do return the extracted data
				return txs, err
			}
			// Append the pending ones
			if endPending {
				pendingTxs, err := f.pendingTransactions()
				if err != nil {
					// if an error occurs during extraction, we do return the extracted data
					return txs, err
				}
				txs = append(txs, pendingTxs...)
				if checkLimit() {
					return txs, nil
				}
			}
			return txs, nil
		}
	}
}

// rangeTransactionsAsync retrieves block-range logs that match the filter criteria asynchronously,
// it creates and returns two channels: one for delivering transaction data, and one for reporting errors.
func (f *TxFilter) rangeTransactionsAsync(ctx context.Context, limitChan chan bool) (chan *ethapi.RPCTransaction, chan error) {
	var (
		txChan  = make(chan *ethapi.RPCTransaction)
		errChan = make(chan error)
	)

	go func() {
		defer func() {
			close(errChan)
			close(txChan)
		}()

		// Gather all indexed logs, and finish with non indexed ones
		var (
			end            = uint64(f.end)
			size, sections = f.sys.backend.TxBloomStatus()
			err            error
		)

		if indexed := sections * size; indexed > uint64(f.begin) {
			if indexed > end {
				indexed = end + 1
			}
			if err = f.indexedTransactions(ctx, indexed-1, limitChan, txChan); err != nil {
				errChan <- err
				return
			}
		}

		if err := f.unindexedTransactions(ctx, end, limitChan, txChan); err != nil {
			errChan <- err
			return
		}

		errChan <- nil
	}()

	return txChan, errChan
}

// indexedTransactions returns the transactions matching the filter criteria based on the bloom
// bits indexed available locally or via the network.
func (f *TxFilter) indexedTransactions(ctx context.Context, end uint64, limitChan chan bool, txChan chan *ethapi.RPCTransaction) error {
	// Create a matcher session and request servicing from the backend
	matches := make(chan uint64, 64)

	session, err := f.matcher.Start(ctx, uint64(f.begin), end, matches)
	if err != nil {
		return err
	}
	defer session.Close()

	f.sys.backend.TxServiceFilter(ctx, session)

	for {
		select {
		case number, ok := <-matches:
			// Abort if all matches have been fulfilled
			if !ok {
				err := session.Error()
				if err == nil {
					f.begin = int64(end) + 1
				}
				return err
			}
			f.begin = int64(number) + 1

			// Retrieve the suggested block and pull any truly matching transactions
			header, err := f.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(number))
			if header == nil || err != nil {
				return err
			}
			found, err := f.checkMatches(ctx, header)
			if err != nil {
				return err
			}
			for _, tx := range found {
				txChan <- tx
			}
		case <-limitChan:
			log.Info("Indexed chan received")
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// unindexedTransactions returns the transctions matching the filter criteria based on raw block
// iteration and bloom matching.
func (f *TxFilter) unindexedTransactions(ctx context.Context, end uint64, limitChan chan bool, txChan chan *ethapi.RPCTransaction) error {
	for ; f.begin <= int64(end); f.begin++ {
		header, err := f.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(f.begin))
		if header == nil || err != nil {
			return err
		}
		found, err := f.blockTransactions(ctx, header)
		if err != nil {
			return err
		}
		for _, tx := range found {
			select {
			case txChan <- tx:
			case <-ctx.Done():
				return ctx.Err()
			case <-limitChan:
				log.Info("Unindexed chan received")
				return nil
			}
		}
	}
	return nil
}

func (f *TxFilter) filterAddresses() []common.Address {
	return append(f.fromAddresses, f.toAddresses...)
}

func (f *TxFilter) blockTransactions(ctx context.Context, header *types.Header) ([]*ethapi.RPCTransaction, error) {
	bloom := f.sys.backend.GetTxBloom(ctx, header.Hash())
	if bloomTxFilter(bloom, f.filterAddresses(), f.sigHashes) {
		return f.checkMatches(ctx, header)
	}
	return nil, nil
}

// pendingTransactions returns the transactions matching the filter criteria within the pending block.
func (f *TxFilter) pendingTransactions() ([]*ethapi.RPCTransaction, error) {
	block, _ := f.sys.backend.PendingBlockAndReceipts()
	if block == nil {
		return nil, nil
	}

	// calculate the tx bloom filter for the pending block
	signer := types.MakeSigner(f.sys.backend.ChainConfig(), block.Number(), block.Time())
	bloomBytes, err := types.TransactionsBloom(block.Transactions(), signer)
	if err != nil {
		return nil, err
	}

	if bloomTxFilter(types.BytesToBloom(bloomBytes), f.filterAddresses(), f.sigHashes) {
		var rpcTxs []*ethapi.RPCTransaction
		for i, tx := range block.Transactions() {
			rpcTx := ethapi.NewRPCTransaction(tx, block.Header(), uint64(i), f.sys.backend.ChainConfig())
			rpcTxs = append(rpcTxs, &rpcTx)
		}
		return filterTransactionsRPC(rpcTxs, nil, nil, f.fromAddresses, f.toAddresses, f.sigHashes), nil
	}
	return nil, nil
}

// checkMatches checks if the receipts belonging to the given header contain any log events that
// match the filter criteria. This function is called when the bloom filter signals a potential match.
// skipFilter signals all logs of the given block are requested.
func (f *TxFilter) checkMatches(ctx context.Context, header *types.Header) ([]*ethapi.RPCTransaction, error) {
	hash := header.Hash()

	// TODO logs has a cache layer here but for tesing purposes its not needed
	body, err := f.sys.backend.GetBody(ctx, hash, rpc.BlockNumber(header.Number.Uint64()))
	if err != nil {
		return nil, err
	}

	// rpcTxs := make([]ethapi.RPCTransaction, len(body.Transactions))
	var rpcTxs []*ethapi.RPCTransaction
	for i, tx := range body.Transactions {
		rpcTx := ethapi.NewRPCTransaction(tx, header, uint64(i), f.sys.backend.ChainConfig())
		rpcTxs = append(rpcTxs, &rpcTx)
	}

	txs := filterTransactionsRPC(rpcTxs, nil, nil, f.fromAddresses, f.toAddresses, f.sigHashes)
	if len(txs) == 0 {
		return nil, nil
	}

	return txs, nil
}

// filterTransactions creates a slice of logs matching the given criteria.
func filterTransactionsRPC(txs []*ethapi.RPCTransaction, fromBlock, toBlock *big.Int, fromAddresses, toAddresses []common.Address, sigHashes [][]byte) []*ethapi.RPCTransaction {
	var check = func(tx *ethapi.RPCTransaction) bool {
		if fromBlock != nil && fromBlock.Int64() >= 0 && fromBlock.Uint64() > tx.BlockNumber.ToInt().Uint64() {
			return false
		}
		if toBlock != nil && toBlock.Int64() >= 0 && toBlock.Uint64() < tx.BlockNumber.ToInt().Uint64() {
			return false
		}

		if len(fromAddresses) > 0 && !includes(fromAddresses, tx.From) {
			return false
		}

		// To can be nil for contract creation
		if len(toAddresses) > 0 && (tx.To == nil || !includes(toAddresses, *tx.To)) {
			return false
		}

		if sigHashes != nil && len(sigHashes) > 0 {
			var included bool
			for _, sigHash := range sigHashes {
				// Handle non-contract call
				if (tx.Input == nil || len(tx.Input) == 0) && (sigHash == nil || len(sigHash) == 0) {
					included = true
					break
				}
				if bytes.HasPrefix(tx.Input, sigHash) {
					included = true
					break
				}
			}
			if !included {
				return false
			}
		}

		return true
	}

	var ret []*ethapi.RPCTransaction
	for _, tx := range txs {
		if check(tx) {
			ret = append(ret, tx)
		}
	}
	return ret
}

// bloomTxFilter checks a bloom filter for transactions that match the given criteria
// addresses covers the sender and to of the transaction
func bloomTxFilter(bloom types.Bloom, addresses []common.Address, sigHashes [][]byte) bool {
	if len(addresses) > 0 {
		var included bool
		for _, addr := range addresses {
			if types.BloomLookup(bloom, addr) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}

	if len(sigHashes) > 0 {
		var included bool
		for _, sigHash := range sigHashes {
			// TODO should this be allowed it could impact performance
			if sigHash == nil || len(sigHash) == 0 {
				included = true
				break
			}
			if bloom.Test(sigHash) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}

	return true
}
