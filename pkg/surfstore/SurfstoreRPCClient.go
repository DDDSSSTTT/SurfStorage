package surfstore

import (
	context "context"
	"errors"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddr string
	BaseDir       string
	BlockSize     int
}

func (surfClient *RPCClient) DialBlockStore(blockStoreAddr string) (
	conn *grpc.ClientConn, bs_client BlockStoreClient, ctx context.Context,
	cancel context.CancelFunc, err error) {
	conn, err = grpc.Dial(blockStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	bs_client = NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	return
}
func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, c, ctx, cancel, err := surfClient.DialBlockStore(blockStoreAddr)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	conn, c, ctx, cancel, err := surfClient.DialBlockStore(blockStoreAddr)
	defer cancel()
	S, err := c.PutBlock(ctx, block)
	*succ = S.Flag
	if err != nil {
		return err
	}
	if S.GetFlag() != true {
		return errors.New("Client PutBlock fails somehow")
	}
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	conn, c, ctx, cancel, err := surfClient.DialBlockStore(blockStoreAddr)
	defer cancel()

	inlist, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	*blockHashesOut = inlist.Hashes
	if err != nil {
		return err
	}
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	conn, c, ctx, cancel, err := surfClient.DialMetaStore()
	defer cancel()
	infomap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*serverFileInfoMap = infomap.FileInfoMap

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	conn, c, ctx, cancel, err := surfClient.DialMetaStore()
	defer cancel()
	version, err := c.UpdateFile(ctx, fileMetaData)

	if err != nil {
		conn.Close()
		return err
	}
	*latestVersion = version.GetVersion()
	block_store_addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	if *latestVersion != -1 {
		var in_list []string
		var success = Success{Flag: true}
		this_addr := block_store_addr.GetAddr()
		err := surfClient.HasBlocks(fileMetaData.BlockHashList, this_addr, &in_list)
		if err != nil {
			return err
		}
		for _, each_hash := range fileMetaData.BlockHashList {
			if !contains(in_list, each_hash) {
				this_byte := GetBlockHashBytes([]byte(each_hash))
				this_len := len(this_byte)
				this_block := Block{BlockData: this_byte, BlockSize: int32(this_len)}
				surfClient.PutBlock(&this_block, this_addr, &success.Flag)
			}
		}
	}
	// close the connection
	return conn.Close()
}
func (surfClient *RPCClient) DialMetaStore() (
	conn *grpc.ClientConn, mt_client MetaStoreClient, ctx context.Context,
	cancel context.CancelFunc, err error) {
	conn, err = grpc.Dial(surfClient.MetaStoreAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return
	}
	mt_client = NewMetaStoreClient(conn)

	// perform the call
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	return
}
func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
	// connect to the server
	conn, c, ctx, cancel, err := surfClient.DialMetaStore()
	defer cancel()
	addr, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockStoreAddr = addr.Addr

	// close the connection
	return conn.Close()
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(hostPort, baseDir string, blockSize int) RPCClient {

	return RPCClient{
		MetaStoreAddr: hostPort,
		BaseDir:       baseDir,
		BlockSize:     blockSize,
	}
}
