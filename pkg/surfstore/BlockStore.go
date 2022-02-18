package surfstore

import (
	context "context"
	"errors"
	"sync"
	"time"
)

type BlockStore struct {
	BlockMap map[string]*Block
	mu       sync.Mutex
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	time.Sleep(5 * time.Millisecond)
	bs.mu.Lock()
	this_hash := blockHash.GetHash()
	block, ok := bs.BlockMap[this_hash]
	if ok != true {

		return &Block{}, errors.New("BS: GetBlock - Hash value not found in the BlockMap\n")
	}
	this_byte := block.GetBlockData()
	this_len := block.GetBlockSize()
	bs.mu.Unlock()
	return &Block{BlockData: this_byte, BlockSize: this_len}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.mu.Lock()
	blkdata := block.GetBlockData()
	hash_string := GetBlockHashString(blkdata)
	bs.BlockMap[hash_string] = block
	bs.mu.Unlock()
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	bs.mu.Lock()
	output := BlockHashes{}
	for _, each_hash := range blockHashesIn.Hashes {
		_, ok := bs.BlockMap[each_hash]
		if ok == true {
			output.Hashes = append(output.Hashes, each_hash)
		}
	}
	bs.mu.Unlock()
	return &output, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
