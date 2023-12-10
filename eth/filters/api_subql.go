package filters

import (
	"context"
	"encoding/json"
	"math/big"

	subqlD "bitbucket.org/onfinalitydev/dict-takoyaki/subql"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type Header struct {
	Hash       common.Hash  `json:"hash"`
	Number     *hexutil.Big `json:"number"`
	ParentHash common.Hash  `json:"parentHash"`
}

type BlockResult struct {
	Blocks      []*Block
	BlockRange  [2]uint64 // Tuple [start, end]
	GenesisHash string
}

type Block struct {
	Header       *Header
	Transactions []ethapi.RPCTransaction
	Logs         []types.Log
}

type BlockRequest struct {
	FromBlock   *rpc.BlockNumber `json:"fromBlock"`
	ToBlock     *rpc.BlockNumber `json:"toBlock"`
	Limit       *big.Int         `json:"limit"`
	BlockFilter EntityFilter     `json:"blockFilter,omitempty"`
	// FieldSelector FieldSelector `json:"fieldSelector"`
}

type FieldFilter map[string][]interface{}

type EntityFilter map[string][]FieldFilter

type SubqlAPI struct {
	sys           *FilterSystem
	backend       ethapi.Backend
	genesisHeader *types.Header
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

func (api *SubqlAPI) FilterBlocksCapabilities(ctx context.Context) (*subqlD.Capability, error) {
	res := &subqlD.Capability{
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

	// TODO need to use FinalizedBlockHeight, for test network "Finalized" doesn't work
	endBlock, err := api.backend.BlockByNumber(ctx, rpc.LatestBlockNumber)
	if err != nil {
		return nil, err
	}

	res.AvailableBlocks = []struct {
		StartHeight int `json:"startHeight"`
		EndHeight   int `json:"endHeight"`
	}{
		{StartHeight: int(api.genesisHeader.Number.Uint64()), EndHeight: int(endBlock.NumberU64())},
	}

	res.GenesisHash = api.genesisHeader.Hash().Hex()

	return res, nil
}

func (api *SubqlAPI) FilterBlocks(ctx context.Context, blockFilter BlockFilter) (*BlockResult, error) {

	err := api.getGenesisHeader(ctx)
	if err != nil {
		return nil, err
	}

	result := &BlockResult{
		GenesisHash: api.genesisHeader.Hash().Hex(),
		BlockRange:  [2]uint64{0, 0}, // TODO actual range
	}

	var logResults []*types.Log
	var txResults []*ethapi.RPCTransaction

	for _, logFilter := range blockFilter.Logs {
		f := api.sys.NewRangeFilter(logFilter.FromBlock.Int64(), logFilter.ToBlock.Int64(), logFilter.Addresses, logFilter.Topics)
		results, err := f.Logs(ctx)
		if err != nil {
			return nil, err
		}

		logResults = append(logResults, results...)
	}

	for _, txFilter := range blockFilter.Transactions {
		f := api.sys.NewTxRangeFilter(txFilter.FromBlock.Int64(), txFilter.ToBlock.Int64(), txFilter.FromAddresses, txFilter.ToAddresses, txFilter.SigHashes)
		results, err := f.Transactions(ctx)
		if err != nil {
			return nil, err
		}

		txResults = append(txResults, results...)
	}

	result.Blocks, err = api.buildBlocks(ctx, txResults, logResults)
	if err != nil {
		return nil, err
	}

	log.Info("NUM RESULTS", "txs", len(txResults), "logs", len(logResults), "blocks", len(result.Blocks))

	// TODO implement limit

	return result, nil
}

func (api *SubqlAPI) buildBlocks(ctx context.Context, txs []*ethapi.RPCTransaction, logs []*types.Log) ([]*Block, error) {
	grouped := map[uint64]*Block{}

	for _, tx := range txs {
		num := tx.BlockNumber.ToInt().Uint64()
		block, ok := grouped[num]
		if !ok {
			header, err := api.getHeader(ctx, rpc.BlockNumber(tx.BlockNumber.ToInt().Uint64()))
			if err != nil {
				return nil, err
			}

			grouped[num] = &Block{
				Header:       header,
				Transactions: []ethapi.RPCTransaction{*tx},
				Logs:         []types.Log{},
			}
		} else {
			block.Transactions = append(block.Transactions, *tx)
		}
	}

	for _, log := range logs {
		block, ok := grouped[log.BlockNumber]
		if !ok {
			header, err := api.getHeader(ctx, rpc.BlockNumber(log.BlockNumber))
			if err != nil {
				return nil, err
			}

			grouped[log.BlockNumber] = &Block{
				Header:       header,
				Transactions: []ethapi.RPCTransaction{},
				Logs:         []types.Log{*log},
			}
		} else {
			block.Logs = append(block.Logs, *log)
		}
	}

	res := make([]*Block, 0, len(grouped))

	for _, b := range grouped {
		res = append(res, b)
	}

	return res, nil
}

func (api *SubqlAPI) getGenesisHeader(ctx context.Context) error {
	if api.genesisHeader == nil {
		genesisBlock, err := api.backend.BlockByNumber(ctx, rpc.EarliestBlockNumber)
		if err != nil {
			return err
		}
		api.genesisHeader = genesisBlock.Header()
	}

	return nil
}

func (api *SubqlAPI) getHeader(ctx context.Context, blockNum rpc.BlockNumber) (*Header, error) {
	fullHeader, err := api.sys.backend.HeaderByNumber(ctx, blockNum)
	if err != nil {
		return nil, err
	}

	return &Header{
		Hash:       fullHeader.Hash(),
		ParentHash: fullHeader.ParentHash,
		Number:     (*hexutil.Big)(fullHeader.Number),
	}, nil
}

type BlockFilter struct {
	FromBlock    *big.Int
	ToBlock      *big.Int
	Transactions []ethereum.TxFilterQuery
	Logs         []ethereum.FilterQuery
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

	if logsFilter, ok := raw.BlockFilter["logs"]; ok {
		args.Logs = []ethereum.FilterQuery{}

		for _, logFilter := range logsFilter {
			addresses, err := decodeAddresses(logFilter["address"])
			if err != nil {
				return err
			}

			topics, err := decodeTopics(logFilter)
			if err != nil {
				return err
			}

			filterQuery := ethereum.FilterQuery{
				FromBlock: args.FromBlock,
				ToBlock:   args.ToBlock,
				Addresses: addresses,
				Topics:    [][]common.Hash{topics},
			}

			args.Logs = append(args.Logs, filterQuery)
		}
	}

	if txsFilter, ok := raw.BlockFilter["transactions"]; ok {
		args.Logs = []ethereum.FilterQuery{}

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

func decodeTopics(f FieldFilter) ([]common.Hash, error) {
	rawTopics := []interface{}{f["topic0"], f["topic1"], f["topic2"], f["topic3"]}
	result := make([]common.Hash, 4)

	for i, t := range rawTopics {
		switch topic := t.(type) {
		case nil:
			// nothing
		case string:
			parsed, err := decodeTopic(topic)
			if err != nil {
				return nil, err
			}
			result[i] = parsed
		case common.Hash:
			result[i] = topic
		default:
			return nil, errInvalidTopic
		}
	}

	return result, nil
}
