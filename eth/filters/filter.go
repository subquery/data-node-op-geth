// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package filters

import (
	"context"
	"errors"
	"math"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/bloombits"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

// Filter can be used to retrieve and filter logs.
type Filter struct {
	sys *FilterSystem

	addresses []common.Address
	topics    [][]common.Hash

	block             *common.Hash // Block hash if filtering a single block
	begin, end, limit int64        // Range interval if filtering multiple blocks

	childFilters []*Filter

	matcher *bloombits.Matcher
}

// NewRangeFilter creates a new filter which uses a bloom filter on blocks to
// figure out whether a particular block is interesting or not.
func (sys *FilterSystem) NewRangeFilter(begin, end int64, addresses []common.Address, topics [][]common.Hash) *Filter {
	return sys.NewRangeFilterWithLimit(begin, end, 0, addresses, topics)
}

// NewRangeFilter creates a new filter which uses a bloom filter on blocks to
// figure out whether a particular block is interesting or not.
func (sys *FilterSystem) NewRangeFilterWithLimit(begin, end, limit int64, addresses []common.Address, topics [][]common.Hash) *Filter {
	// Flatten the address and topic filter clauses into a single bloombits filter
	// system. Since the bloombits are not positional, nil topics are permitted,
	// which get flattened into a nil byte slice.
	var filters [][][]byte
	if len(addresses) > 0 {
		filter := make([][]byte, len(addresses))
		for i, address := range addresses {
			filter[i] = address.Bytes()
		}
		filters = append(filters, filter)
	}
	for _, topicList := range topics {
		filter := make([][]byte, len(topicList))
		for i, topic := range topicList {
			filter[i] = topic.Bytes()
		}
		filters = append(filters, filter)
	}
	size, _ := sys.backend.BloomStatus()

	// Create a generic filter and convert it into a range filter
	filter := newFilter(sys, addresses, topics)

	filter.matcher = bloombits.NewMatcher(size, filters)
	filter.begin = begin
	filter.end = end
	filter.limit = limit

	return filter
}

func (sys *FilterSystem) NewBatchRangeFilter(filters []*Filter) (*Filter, error) {
	if len(filters) == 0 {
		return nil, errors.New("At least one filter is required")
	}

	var begin, end, limit int64
	var addresses []common.Address
	var topics [][]common.Hash

	for _, f := range filters {
		if f.block != nil {
			return nil, errors.New("Cannot batch with range filter")
		}
		if begin == 0 {
			begin = f.begin
		} else {
			begin = int64(math.Min(float64(begin), float64(f.begin)))
		}
		end = int64(math.Max(float64(end), float64(f.end)))
		limit = int64(math.Max(float64(limit), float64(f.limit)))
		addresses = append(addresses, f.addresses...)
		topics = zipTopics(topics, f.topics)
	}

	batched := sys.NewRangeFilterWithLimit(begin, end, limit, addresses, topics)
	batched.childFilters = filters

	return batched, nil
}

// NewBlockFilter creates a new filter which directly inspects the contents of
// a block to figure out whether it is interesting or not.
func (sys *FilterSystem) NewBlockFilter(block common.Hash, addresses []common.Address, topics [][]common.Hash) *Filter {
	// Create a generic filter and convert it into a block filter
	filter := newFilter(sys, addresses, topics)
	filter.block = &block
	return filter
}

// newFilter creates a generic filter that can either filter based on a block hash,
// or based on range queries. The search criteria needs to be explicitly set.
func newFilter(sys *FilterSystem, addresses []common.Address, topics [][]common.Hash) *Filter {
	return &Filter{
		sys:       sys,
		addresses: addresses,
		topics:    topics,
	}
}

// Logs searches the blockchain for matching log entries, returning all from the
// first block that contains matches, updating the start of the filter accordingly.
func (f *Filter) Logs(ctx context.Context) ([]*types.Log, error) {
	// If we're doing singleton block filtering, execute and return
	if f.block != nil {
		header, err := f.sys.backend.HeaderByHash(ctx, *f.block)
		if err != nil {
			return nil, err
		}
		if header == nil {
			return nil, errors.New("unknown block")
		}
		return f.blockLogs(ctx, header)
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
		return f.pendingLogs(), nil
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

	logChan, errChan := f.rangeLogsAsync(ctx, limitChan)
	var logs []*types.Log

	// TODO limit is for number of blocks not number of logs
	var checkLimit = func() bool {
		if f.limit == 0 {
			return false
		}

		// Shortcut to check limit, if we have less tx than the limit there is no need to check unique blocks
		if len(logs) < int(f.limit) {
			return false
		}

		blocks := map[uint64]bool{}
		for _, log := range logs {
			blocks[log.BlockNumber] = true
		}

		if len(blocks) >= int(f.limit) {
			limitChan <- true
			return true
		}

		return false
	}

	for {
		select {
		case log := <-logChan:
			logs = append(logs, log)
			if checkLimit() {
				return logs, nil
			}
		case err := <-errChan:
			if err != nil {
				// if an error occurs during extraction, we do return the extracted data
				return logs, err
			}
			// Append the pending ones
			if endPending {
				pendingLogs := f.pendingLogs()
				logs = append(logs, pendingLogs...)
				if checkLimit() {
					return logs, nil
				}
			}
			return logs, nil
		}
	}
}

// rangeLogsAsync retrieves block-range logs that match the filter criteria asynchronously,
// it creates and returns two channels: one for delivering log data, and one for reporting errors.
func (f *Filter) rangeLogsAsync(ctx context.Context, limitChan chan bool) (chan *types.Log, chan error) {
	var (
		logChan = make(chan *types.Log)
		errChan = make(chan error)
	)

	go func() {
		defer func() {
			close(errChan)
			close(logChan)
		}()

		// Gather all indexed logs, and finish with non indexed ones
		var (
			end            = uint64(f.end)
			size, sections = f.sys.backend.BloomStatus()
			err            error
		)
		if indexed := sections * size; indexed > uint64(f.begin) {
			if indexed > end {
				indexed = end + 1
			}
			if err = f.indexedLogs(ctx, indexed-1, logChan, limitChan); err != nil {
				errChan <- err
				return
			}
		}

		if err := f.unindexedLogs(ctx, end, logChan, limitChan); err != nil {
			errChan <- err
			return
		}

		errChan <- nil
	}()

	return logChan, errChan
}

// indexedLogs returns the logs matching the filter criteria based on the bloom
// bits indexed available locally or via the network.
func (f *Filter) indexedLogs(ctx context.Context, end uint64, logChan chan *types.Log, limitChan chan bool) error {
	// Create a matcher session and request servicing from the backend
	matches := make(chan uint64, 64)

	session, err := f.matcher.Start(ctx, uint64(f.begin), end, matches)
	if err != nil {
		return err
	}
	defer session.Close()

	f.sys.backend.ServiceFilter(ctx, session)

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

			// Retrieve the suggested block and pull any truly matching logs
			header, err := f.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(number))
			if header == nil || err != nil {
				return err
			}
			found, err := f.checkMatches(ctx, header)
			if err != nil {
				return err
			}
			for _, log := range found {
				logChan <- log
			}
		case <-limitChan:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// unindexedLogs returns the logs matching the filter criteria based on raw block
// iteration and bloom matching.
func (f *Filter) unindexedLogs(ctx context.Context, end uint64, logChan chan *types.Log, limitChan chan bool) error {
	for ; f.begin <= int64(end); f.begin++ {
		header, err := f.sys.backend.HeaderByNumber(ctx, rpc.BlockNumber(f.begin))
		if header == nil || err != nil {
			return err
		}
		found, err := f.blockLogs(ctx, header)
		if err != nil {
			return err
		}
		for _, log := range found {
			select {
			case logChan <- log:
			case <-ctx.Done():
				return ctx.Err()
			case <-limitChan:
				return nil
			}
		}
	}
	return nil
}

// blockLogs returns the logs matching the filter criteria within a single block.
func (f *Filter) blockLogs(ctx context.Context, header *types.Header) ([]*types.Log, error) {
	if bloomFilter(header.Bloom, f.addresses, f.topics) {
		return f.checkMatches(ctx, header)
	}
	return nil, nil
}

// checkMatches checks if the receipts belonging to the given header contain any log events that
// match the filter criteria. This function is called when the bloom filter signals a potential match.
// skipFilter signals all logs of the given block are requested.
func (f *Filter) checkMatches(ctx context.Context, header *types.Header) ([]*types.Log, error) {
	hash := header.Hash()
	// Logs in cache are partially filled with context data
	// such as tx index, block hash, etc.
	// Notably tx hash is NOT filled in because it needs
	// access to block body data.
	cached, err := f.sys.cachedLogElem(ctx, hash, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	logs := f.childFilterLogs(cached.logs)
	if len(logs) == 0 {
		return nil, nil
	}
	// Most backends will deliver un-derived logs, but check nevertheless.
	if len(logs) > 0 && logs[0].TxHash != (common.Hash{}) {
		return logs, nil
	}

	body, err := f.sys.cachedGetBody(ctx, cached, hash, header.Number.Uint64())
	if err != nil {
		return nil, err
	}
	for i, log := range logs {
		// Copy log not to modify cache elements
		logcopy := *log
		logcopy.TxHash = body.Transactions[logcopy.TxIndex].Hash()
		logs[i] = &logcopy
	}
	return logs, nil
}

// pendingLogs returns the logs matching the filter criteria within the pending block.
func (f *Filter) pendingLogs() []*types.Log {
	block, receipts := f.sys.backend.PendingBlockAndReceipts()
	if block == nil || receipts == nil {
		return nil
	}
	if bloomFilter(block.Bloom(), f.addresses, f.topics) {
		var unfiltered []*types.Log
		for _, r := range receipts {
			unfiltered = append(unfiltered, r.Logs...)
		}
		return f.childFilterLogs(unfiltered)
	}
	return nil
}

// includes returns true if the element is present in the list.
func includes[T comparable](things []T, element T) bool {
	for _, thing := range things {
		if thing == element {
			return true
		}
	}
	return false
}

func includesFn[T any](things []T, matcher func(thing T) bool) bool {
	for _, thing := range things {
		if matcher(thing) {
			return true
		}
	}
	return false
}

func (f *Filter) childFilterLogs(logs []*types.Log) []*types.Log {
	if f.childFilters == nil || len(f.childFilters) == 0 {
		return filterLogs(logs, nil, nil, f.addresses, f.topics)
	}

	var ret []*types.Log
	for _, log := range logs {
		for _, f := range f.childFilters {
			if filterLog(log, nil, nil, f.addresses, f.topics) {
				ret = append(ret, log)
				// Break the inner loop
				break
			}
		}
	}
	return ret
}

// filterLogs creates a slice of logs matching the given criteria.
func filterLogs(logs []*types.Log, fromBlock, toBlock *big.Int, addresses []common.Address, topics [][]common.Hash) []*types.Log {
	var ret []*types.Log
	for _, log := range logs {
		if filterLog(log, fromBlock, toBlock, addresses, topics) {
			ret = append(ret, log)
		}
	}
	return ret
}

func filterLog(log *types.Log, fromBlock, toBlock *big.Int, addresses []common.Address, topics [][]common.Hash) bool {
	if fromBlock != nil && fromBlock.Int64() >= 0 && fromBlock.Uint64() > log.BlockNumber {
		return false
	}
	if toBlock != nil && toBlock.Int64() >= 0 && toBlock.Uint64() < log.BlockNumber {
		return false
	}
	if len(addresses) > 0 && !includes(addresses, log.Address) {
		return false
	}
	// If the to filtered topics is greater than the amount of topics in logs, skip.
	if len(topics) > len(log.Topics) {
		return false
	}
	for i, sub := range topics {
		if len(sub) == 0 {
			continue // empty rule set == wildcard
		}
		if !includes(sub, log.Topics[i]) {
			return false
		}
	}
	return true
}

func bloomFilter(bloom types.Bloom, addresses []common.Address, topics [][]common.Hash) bool {
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

	for _, sub := range topics {
		included := len(sub) == 0 // empty rule set == wildcard
		for _, topic := range sub {
			if types.BloomLookup(bloom, topic) {
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

func resolveSpecial(sys *FilterSystem, ctx context.Context, number int64) (int64, error) {
	var hdr *types.Header
	switch number {
	case rpc.LatestBlockNumber.Int64(), rpc.PendingBlockNumber.Int64():
		// we should return head here since we've already captured
		// that we need to get the pending logs in the pending boolean above
		hdr, _ = sys.backend.HeaderByNumber(ctx, rpc.LatestBlockNumber)
		if hdr == nil {
			return 0, errors.New("latest header not found")
		}
	case rpc.FinalizedBlockNumber.Int64():
		hdr, _ = sys.backend.HeaderByNumber(ctx, rpc.FinalizedBlockNumber)
		if hdr == nil {
			return 0, errors.New("finalized header not found")
		}
	case rpc.SafeBlockNumber.Int64():
		hdr, _ = sys.backend.HeaderByNumber(ctx, rpc.SafeBlockNumber)
		if hdr == nil {
			return 0, errors.New("safe header not found")
		}
	default:
		return number, nil
	}
	return hdr.Number.Int64(), nil
}

// Function to zip two arrays of arrays
func zipTopics(topics1, topics2 [][]common.Hash) [][]common.Hash {
	// Ensure both arrays have the same length
	minLen := len(topics1)
	if len(topics2) < minLen {
		minLen = len(topics2)
	}

	// Initialize the zipped array
	zipped := make([][]common.Hash, minLen)

	// Zip the arrays
	for i := 0; i < minLen; i++ {
		zipped[i] = make([]common.Hash, len(topics1[i])+len(topics2[i]))
		copy(zipped[i], topics1[i])
		copy(zipped[i][len(topics1[i]):], topics2[i])
	}

	return zipped
}
