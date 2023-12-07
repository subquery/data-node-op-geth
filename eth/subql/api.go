package subql

import (
	"context"

	subqlD "bitbucket.org/onfinalitydev/dict-takoyaki/subql"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/filters"
	"github.com/ethereum/go-ethereum/internal/ethapi"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type SubqlAPI struct {
	sys           *filters.FilterSystem
	backend       ethapi.Backend
	genesisHeader *types.Header
}

func NewSubqlApi(sys *filters.FilterSystem, backend ethapi.Backend) *SubqlAPI {
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
			"transaction": {"to", "from", "data"},
			"logs":        {"address", "topics0", "topics1", "topics2", "topics3"},
		},
		SupportedResponses: []string{"basic", "complete"},
	}

	if api.genesisHeader == nil {
		genesisBlock, err := api.backend.BlockByNumber(ctx, rpc.EarliestBlockNumber)
		if err != nil {
			return nil, err
		}
		api.genesisHeader = genesisBlock.Header()
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
