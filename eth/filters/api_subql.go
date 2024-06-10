package filters

import (
	"context"
	"encoding/json"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"math"
	"math/big"
	"sort"
)

type BlockResult struct {
	Blocks      []*Block        `json:"blocks"`
	BlockRange  [2]*hexutil.Big `json:"blockRange"` // Tuple [start, end]
	GenesisHash string          `json:"genesisHash"`
}

type Block struct {
	Header       *types.Header            `json:"header"`
	Transactions []*ethapi.RPCTransaction `json:"transactions,omitempty"`
	Logs         []*types.Log             `json:"logs,omitempty"`
}

type BlockRequest struct {
	FromBlock     *rpc.BlockNumber `json:"fromBlock"`
	ToBlock       *rpc.BlockNumber `json:"toBlock"`
	Limit         *hexutil.Big     `json:"limit"`
	BlockFilter   EntityFilter     `json:"blockFilter,omitempty"`
	FieldSelector *FieldSelector   `json:"fieldSelector"`
}

type LogsSelector struct {
	Transaction bool `json:"transaction"`
	// TODO add specific fields
}

type TransactionsSelector struct {
	Log bool `json:"log"`
	// TODO add specific fields
}

type FieldSelector struct {
	Logs         *LogsSelector         `json:"logs"`
	Transactions *TransactionsSelector `json:"transactions"`
}

type FieldFilter map[string][]interface{}

type EntityFilter map[string][]FieldFilter

type SubqlAPI struct {
	sys           *FilterSystem
	backend       ethapi.Backend
	genesisHeader *types.Header
}

type Capability struct {
	AvailableBlocks []struct {
		StartHeight int `json:"startHeight"`
		EndHeight   int `json:"endHeight"`
	} `json:"availableBlocks"`
	Filters            map[string][]string `json:"filters"`
	SupportedResponses []string            `json:"supportedResponses"`
	GenesisHash        string              `json:"genesisHash"`
	ChainId            string              `json:"chainId"`
}

type block struct {
	Header       *types.Header
	Transactions map[hexutil.Uint64]*ethapi.RPCTransaction
	Logs         map[uint]*types.Log
}

type blocks = map[uint64]*block

func values[M ~map[K]V, K comparable, V any](m M) []V {
	r := make([]V, 0, len(m))
	for _, v := range m {
		r = append(r, v)
	}
	return r
}

func (b *block) toBlock() *Block {

	txs := values(b.Transactions)

	sort.Slice(txs, func(i, j int) bool {
		return *txs[i].TransactionIndex < *txs[j].TransactionIndex
	})

	logs := values(b.Logs)
	sort.Slice(logs, func(i, j int) bool {
		return logs[i].Index < logs[j].Index
	})

	return &Block{
		Header:       b.Header,
		Transactions: txs,
		Logs:         logs,
	}
}

func NewSubqlApi(sys *FilterSystem, backend ethapi.Backend) *SubqlAPI {
	log.Info("NewSubqlApi init")
	api := &SubqlAPI{
		sys,
		backend,
		nil,
	}

	return api
}

func (api *SubqlAPI) FilterBlocksCapabilities(ctx context.Context) (*Capability, error) {
	res := &Capability{
		Filters: map[string][]string{
			"transactions": {"from", "to", "data"},
			"logs":         {"address", "topics0", "topics1", "topics2", "topics3"},
		},
		SupportedResponses: []string{"basic", "complete"},
	}

	err := api.getGenesisHeader(ctx)
	if err != nil {
		return nil, err
	}

	res.AvailableBlocks = []struct {
		StartHeight int `json:"startHeight"`
		EndHeight   int `json:"endHeight"`
	}{
		{StartHeight: int(api.genesisHeader.Number.Uint64()), EndHeight: int(api.endHeight())},
	}

	res.GenesisHash = api.genesisHeader.Hash().Hex()
	res.ChainId = api.backend.ChainConfig().ChainID.String()

	return res, nil
}

func (api *SubqlAPI) FilterBlocks(ctx context.Context, blockFilter BlockFilter) (*BlockResult, error) {
	// TODO validate block range within endHeight

	err := api.getGenesisHeader(ctx)
	if err != nil {
		return nil, err
	}

	result := &BlockResult{
		GenesisHash: api.genesisHeader.Hash().Hex(),
	}

	logResults := []*types.Log{}

	if blockFilter.Logs != nil && len(blockFilter.Logs) > 0 {
		var rangeFilters []*Filter

		for _, logFilter := range blockFilter.Logs {
			rangeFilters = append(rangeFilters, api.sys.NewRangeFilterWithLimit(logFilter.FromBlock.Int64(), logFilter.ToBlock.Int64(), blockFilter.Limit, logFilter.Addresses, logFilter.Topics))
		}

		logf, err := api.sys.NewBatchRangeFilter(rangeFilters)
		if err != nil {
			return nil, err
		}

		logResults, err = logf.Logs(ctx)
		if err != nil {
			return nil, err
		}
	}

	txResults := []*ethapi.RPCTransaction{}
	if blockFilter.Transactions != nil && len(blockFilter.Transactions) > 0 {
		var rangeFilters []*TxFilter

		for _, txFilter := range blockFilter.Transactions {
			rangeFilters = append(rangeFilters, api.sys.NewTxRangeFilter(txFilter.FromBlock.Int64(), txFilter.ToBlock.Int64(), blockFilter.Limit, txFilter.FromAddresses, txFilter.ToAddresses, txFilter.SigHashes))
		}

		txf, err := api.sys.NewBatchTxRangeFilter(rangeFilters)
		if err != nil {
			return nil, err
		}
		txResults, err = txf.Transactions(ctx)
		if err != nil {
			return nil, err
		}
	}

	result.Blocks, err = api.buildBlocks(ctx, txResults, logResults, blockFilter.Limit, blockFilter.FieldSelector)
	if err != nil {
		return nil, err
	}

	// TODO Is this the right range? Its the range of results be we could have searched further
	result.BlockRange = [2]*hexutil.Big{
		(*hexutil.Big)(blockFilter.FromBlock),
		(*hexutil.Big)(big.NewInt(int64(api.endHeight()))),
	}
	// result.BlockRange = [2]*hexutil.Big{
	// 	result.Blocks[0].Header.Number,
	// 	result.Blocks[len(result.Blocks)-1].Header.Number,
	// }

	log.Info("NUM RESULTS", "txs", len(txResults), "logs", len(logResults), "blocks", len(result.Blocks))

	return result, nil
}

// buildBlocks assembles the filtered logs/transactions into the correct Block structure
func (api *SubqlAPI) buildBlocks(ctx context.Context, txs []*ethapi.RPCTransaction, logs []*types.Log, limit int64, fieldSelector *FieldSelector) ([]*Block, error) {
	grouped := blocks{}

	for _, log := range logs {
		api.blocksAddLog(ctx, &grouped, log)
	}

	for _, tx := range txs {
		api.blocksAddTx(ctx, &grouped, tx)
	}

	// Limit the results size
	capacity := len(grouped)
	if limit > 0 {
		capacity = int(math.Max(float64(capacity), float64(limit)))
	}

	// Sort the keys (block heights)
	keys := make([]uint64, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	// Convert the map to an array
	res := make([]*Block, 0, capacity)
	for i, k := range keys {
		if i > capacity {
			break
		}

		api.resolveFieldSelector(ctx, fieldSelector, grouped[k])

		res = append(res, grouped[k].toBlock())
	}

	return res, nil
}

// Resolves any relevant logs for transactions and transactions for logs if requested by the field selector
func (api *SubqlAPI) resolveFieldSelector(ctx context.Context, fieldSelector *FieldSelector, block *block) error {
	if fieldSelector == nil {
		return nil
	}

	logTxs := []*ethapi.RPCTransaction{}
	txLogs := []*types.Log{}

	// Fill in transactions for logs
	if fieldSelector.Logs != nil && fieldSelector.Logs.Transaction {
		for _, log := range block.Logs {
			if _, ok := block.Transactions[hexutil.Uint64(log.TxIndex)]; !ok {
				_, tx, _, _ /*blockIndex*/, transactionIndex, err := api.backend.GetTransaction(ctx, log.TxHash)
				if err != nil {
					return err
				}
				rpcTx := ethapi.NewRPCTransaction(tx, block.Header, transactionIndex, api.sys.backend.ChainConfig())
				logTxs = append(logTxs, &rpcTx)
			}
		}
	}

	// Fill in logs for transactions
	if fieldSelector.Transactions != nil && fieldSelector.Transactions.Log {
		// Get all the logs for a block
		blockLogs, err := api.backend.GetLogs(ctx, block.Header.Hash(), block.Header.Number.Uint64())
		if err != nil {
			return err
		}
		for _, logs := range blockLogs {
			if len(logs) > 0 {
				if _, ok := block.Transactions[hexutil.Uint64(logs[0].TxIndex)]; ok {
					txLogs = append(txLogs, logs[:]...)
				}
			}
		}
	}

	// Append after resolving so we don't resolve in a loop. e.g log -> transaction -> logs
	for _, tx := range logTxs {
		block.Transactions[*tx.TransactionIndex] = tx
	}

	for _, log := range txLogs {
		block.Logs[log.Index] = log
	}

	return nil
}

func (api *SubqlAPI) blocksAddTx(ctx context.Context, blocks *blocks, tx *ethapi.RPCTransaction) error {
	num := tx.BlockNumber.ToInt().Uint64()
	b, ok := (*blocks)[num]
	if !ok {
		header, err := api.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(tx.BlockNumber.ToInt().Uint64()))
		if err != nil {
			return err
		}

		(*blocks)[num] = &block{
			Header:       header,
			Transactions: map[hexutil.Uint64]*ethapi.RPCTransaction{*tx.TransactionIndex: tx},
			Logs:         map[uint]*types.Log{},
		}
	} else {
		b.Transactions[*tx.TransactionIndex] = tx
	}

	return nil
}

func (api *SubqlAPI) blocksAddLog(ctx context.Context, blocks *blocks, log *types.Log) error {
	b, ok := (*blocks)[log.BlockNumber]
	if !ok {
		header, err := api.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(log.BlockNumber))
		if err != nil {
			return err
		}

		(*blocks)[log.BlockNumber] = &block{
			Header:       header,
			Transactions: map[hexutil.Uint64]*ethapi.RPCTransaction{},
			Logs:         map[uint]*types.Log{log.Index: log},
		}
	} else {
		b.Logs[log.Index] = log
	}

	return nil
}

func (api *SubqlAPI) getGenesisHeader(ctx context.Context) error {
	if api.genesisHeader == nil {
		header, err := api.backend.HeaderByNumber(ctx, rpc.EarliestBlockNumber)
		if err != nil {
			return err
		}
		api.genesisHeader = header
	}

	return nil
}

// endHeight gets the minimum indexed height of transactions and logs bloombits
func (api *SubqlAPI) endHeight() uint64 {
	sizeTx, sectionsTx := api.backend.TxBloomStatus()
	sizeL, sectionsL := api.backend.BloomStatus()
	return uint64(math.Min(float64(sizeTx*sectionsTx), float64(sizeL*sectionsL)))
}

type BlockFilter struct {
	FromBlock     *big.Int
	ToBlock       *big.Int
	Limit         int64
	Transactions  []ethereum.TxFilterQuery
	Logs          []ethereum.FilterQuery
	FieldSelector *FieldSelector
}

func (args *BlockFilter) UnmarshalJSON(data []byte) error {
	type input BlockRequest

	var raw input
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	if raw.FromBlock != nil {
		args.FromBlock = big.NewInt(raw.FromBlock.Int64())
	}

	if raw.ToBlock != nil {
		args.ToBlock = big.NewInt(raw.ToBlock.Int64())
	}

	if raw.Limit != nil {
		args.Limit = raw.Limit.ToInt().Int64()
	}

	if raw.FieldSelector != nil {
		args.FieldSelector = raw.FieldSelector
	}

	if logsFilter, ok := raw.BlockFilter["logs"]; ok {
		args.Logs = []ethereum.FilterQuery{}

		for _, logFilter := range logsFilter {
			addresses, err := decodeAddresses(logFilter["address"])
			if err != nil {
				return err
			}

			topics, err := decodeFilterTopics(logFilter)
			if err != nil {
				return err
			}

			filterQuery := ethereum.FilterQuery{
				FromBlock: args.FromBlock,
				ToBlock:   args.ToBlock,
				Addresses: addresses,
				Topics:    topics,
			}

			args.Logs = append(args.Logs, filterQuery)
		}
	}

	if txsFilter, ok := raw.BlockFilter["transactions"]; ok {
		args.Transactions = []ethereum.TxFilterQuery{}

		for _, txFilter := range txsFilter {
			fromAddresses, err := decodeAddresses(txFilter["from"])
			if err != nil {
				return err
			}

			toAddresses, err := decodeAddresses(txFilter["to"])
			if err != nil {
				return err
			}

			sigHashes, err := decodeSigHashes(txFilter["data"])
			if err != nil {
				return err
			}

			filterQuery := ethereum.TxFilterQuery{
				FromBlock:     args.FromBlock,
				ToBlock:       args.ToBlock,
				FromAddresses: fromAddresses,
				ToAddresses:   toAddresses,
				SigHashes:     sigHashes,
			}

			args.Transactions = append(args.Transactions, filterQuery)
		}
	}

	return nil
}

func decodeFilterTopics(f FieldFilter) ([][]common.Hash, error) {
	rawTopics := []interface{}{f["topics0"], f["topics1"], f["topics2"], f["topics3"]}

	decoded, err := decodeTopics(rawTopics)
	if err != nil {
		return nil, err
	}

	// Remove empty arrays
	filtered := [][]common.Hash{}
	for _, topic := range decoded {
		if len(topic) > 0 {
			filtered = append(filtered, topic)
		}
	}

	return filtered, nil
}
