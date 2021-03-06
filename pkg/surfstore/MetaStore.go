package surfstore

import (
	context "context"
	"log"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil

}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	//TODO:TRY TO UPDATE THE FILE OUT HERE
	log.Printf("Metastore start updating FMDv:%d,mFMMv:%d", fileMetaData.GetVersion(),
		m.FileMetaMap[fileMetaData.Filename].GetVersion())
	if fileMetaData.GetVersion() > m.FileMetaMap[fileMetaData.Filename].GetVersion() {
		log.Printf("Higher Version, push to metamap")
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	} else {
		log.Printf("Lower or equal Version, return-1")
		return &Version{Version: -1}, nil
	}
	return &Version{Version: fileMetaData.GetVersion()}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil

}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
