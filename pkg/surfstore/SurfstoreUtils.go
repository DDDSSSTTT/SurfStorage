package surfstore

import (
	"bufio"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
)

const EX_INDEX int = 101
const EX_REMOTE int = 102

func contains(elems []string, v string) bool {
	for _, s := range elems {
		if v == s {
			return true
		}
	}
	return false
}
func GenerateBlock(file_name string, block_len int) ([]Block, error) {
	f, err := os.Open(file_name)
	if err != nil {
		log.Fatal("Send(Generate) Block: Fail to open " + file_name)
		return nil, err
	}
	// s := bufio.NewScanner(f)
	var output []Block
	r := bufio.NewReader(f)
	for {
		var buf = make([]byte, 0, block_len)
		n, err := io.ReadFull(r, buf[:cap(buf)])
		// buf = buf[:n]
		if err != nil {
			if err == io.EOF {
				break
			}
			if err != io.ErrUnexpectedEOF {
				log.Printf(err.Error())
				break
			}
		}
		new_block := Block{BlockData: buf[:n], BlockSize: int32(n)}

		output = append(output, new_block)

	}
	return output, nil
}
func WriteBlock(file_name string, block Block, block_len int32) error {
	f, err := os.OpenFile(file_name, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("WriteBlock: Fail to open " + file_name)
		return err
	}
	w := bufio.NewWriter(f)
	_, err = w.Write(block.BlockData)
	if err != nil {
		log.Fatal("WriteBlock: Fail to write block of " + file_name)
		return err
	}
	w.Flush()
	return nil
}
func local_map_index_init(localInfoMap map[string]*FileMetaData, client RPCClient) map[string]*FileMetaData {
	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		log.Printf("Unable to ls baseDir")
		log.Fatal(err)
	}
	// Check if every local file is updated in the index.txt & local_map
	var file_name_list []string
	for _, file := range files {
		file_name := file.Name()
		if file_name == DEFAULT_META_FILENAME {
			continue
		}
		file_name_list = append(file_name_list, file_name)
		local_meta, exist := localInfoMap[file_name]
		this_hash_list := make([]string, 1)
		this_hash_list[0] = "up"
		if !exist {
			log.Printf("Map Init: Creating an Entry for " + file_name)
			localInfoMap[file_name] = &FileMetaData{Filename: file_name, Version: 0,
				BlockHashList: this_hash_list}
		}

		real_file_name := ConcatPath(client.BaseDir, file_name)
		list_of_blocks, err := GenerateBlock(real_file_name, client.BlockSize)
		if err != nil {
			log.Fatal("Map Init: Generate Block Failed for: " + real_file_name)
		}
		local_meta = localInfoMap[file_name]
		info_hash_list := local_meta.GetBlockHashList()
		update_version_flag := false
		for i, b := range list_of_blocks {
			this_hash := GetBlockHashString(b.GetBlockData())
			if i >= len(info_hash_list) {
				update_version_flag = true
				info_hash_list = append(info_hash_list, this_hash)
				// log.Printf("Map Init:Info_hash_list on i " + info_hash_list[i])
				continue
			}
			if this_hash != info_hash_list[i] {
				update_version_flag = true
				info_hash_list[i] = this_hash
			}
		}
		if len(info_hash_list) != len(list_of_blocks) {
			update_version_flag = true
			info_hash_list = info_hash_list[:len(list_of_blocks)]
		}
		if update_version_flag {
			local_meta.Version += 1
		}

		local_meta.BlockHashList = info_hash_list

	}

	//Check every file in index.txt exist, if not, create a Tombstone
	for k, local_meta := range localInfoMap {
		if !contains(file_name_list, k) {
			if local_meta.BlockHashList[0] != "0" {
				log.Printf("Map Init: Detect 1st delete for " + local_meta.Filename)
				log.Printf("Map Init: Version + 1 ")
				local_meta.Version += 1

			}
			// Very Tricky here
			local_meta.BlockHashList = []string{"0"}
		}
	}
	return localInfoMap

}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	var blockStoreAddr string
	if err := client.GetBlockStoreAddr(&blockStoreAddr); err != nil {
		log.Fatal(err)
	}
	//Check the local index.txt
	localInfoMap, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		log.Printf("index.txt loading failure")
		os.Exit(EX_INDEX)
	}
	//Parse local dir and index.txt to make it accurate
	localInfoMap = local_map_index_init(localInfoMap, client)
	WriteMetaFile(localInfoMap, client.BaseDir)
	// log.Printf("LocalInfoMap Before (aft local file updated): ")
	// PrintMetaMap(localInfoMap)
	//Check the remote index
	var remoteInfoMap = FileInfoMap{}
	err = client.GetFileInfoMap(&remoteInfoMap.FileInfoMap)
	// log.Printf("Meta Server Before:")
	// PrintMetaMap(remoteInfoMap.FileInfoMap)

	if err != nil {
		log.Printf("[SU165] Remote fileinfomap loading failure")
		log.Printf(err.Error())
		os.Exit(EX_REMOTE)
	}
	// Create a temp block
	var block Block
	//Download: Check if remote server has unsynced file or changed file / unsynced block
	for file_name, meta_data := range remoteInfoMap.FileInfoMap {
		if local_meta_data, exist := localInfoMap[file_name]; !exist {
			log.Printf("Found new file " + file_name + " on meta server,sync this down.")
			//If have unsynced file, download it
			if meta_data.BlockHashList[0] == "0" {
				//Tombstone, do not download, still update the map
				log.Printf("Add a Tombstone to local map for: " + file_name)
				localInfoMap[file_name] = meta_data
				continue
			}
			download_hash_list := meta_data.BlockHashList
			real_file_name := ConcatPath(client.BaseDir, file_name)
			for _, each_hash := range download_hash_list {
				if err := client.GetBlock(each_hash, blockStoreAddr, &block); err != nil {
					log.Printf("Unable to get this_hash " + each_hash)
					log.Fatal(err)
				}
				block_size_32 := int32(client.BlockSize)
				if err = WriteBlock(real_file_name, block, block_size_32); err != nil {
					log.Fatal(err)
				}

			}
			localInfoMap[file_name] = meta_data

		} else {
			//Exist a file with same name
			//version control
			//As we will update the serever in the next block, we donothing here
			if meta_data.Version < local_meta_data.Version {
				//Server V < Local V, do nothing here, upload the local version later
				continue
			} else {
				//Server V >= Local V, need to download and update this file
				log.Printf("Download renewal File from server, Server_V: " + strconv.Itoa(int(meta_data.Version)) +
					" Local_V: " + strconv.Itoa(int(local_meta_data.Version)))
				log.Printf(strconv.FormatBool(meta_data.Version <= local_meta_data.Version))

				real_file_name := ConcatPath(client.BaseDir, file_name)
				if meta_data.BlockHashList[0] == "0" {
					//Tombstone, do not download, still update the map
					log.Printf("Add a Tombstone to local map for: " + file_name)
					localInfoMap[file_name] = meta_data
					log.Printf("Tombstone: Remove " + file_name)
					os.Remove(real_file_name)
					continue
				}

				os.Remove(real_file_name)
				for _, each_hash := range meta_data.BlockHashList {
					// Download update blocks
					if err := client.GetBlock(each_hash, blockStoreAddr, &block); err != nil {
						log.Fatal(err)
					}
					log.Printf("get an renewal block: " + GetBlockHashString(block.GetBlockData()))
					block_size_32 := int32(client.BlockSize)
					if err = WriteBlock(real_file_name, block, block_size_32); err != nil {
						log.Fatal(err)
					}

				}
				// WARNING:Update localinfomap here, need to be careful
				localInfoMap[file_name] = meta_data

			}

		}

	}

	// WriteMetaFile(remoteInfoMap.GetFileInfoMap(), client.BaseDir)
	// PrintMetaMap(localInfoMap)

	//Upload: Locate local file that is unsynced to the server
	var suc Success
	for file_name, meta_data := range localInfoMap {
		if remote_meta_data, exist := remoteInfoMap.FileInfoMap[file_name]; !exist {
			log.Printf("Found new file: " + file_name + " on local client,sync this up.")
			if meta_data.BlockHashList[0] == "0" {
				//Upload a Tombstone to the server
				log.Printf("Upload a Tombstone for: " + file_name)
				// new_version := meta_data.GetVersion() + 1
				//Already plused in local map parsing method
				new_version := meta_data.GetVersion()
				client.UpdateFile(meta_data, &new_version)
				continue
			}
			//Have unsynced file, upload it
			this_blocks, err := GenerateBlock(ConcatPath(client.BaseDir, file_name), client.BlockSize)
			if err != nil {
				log.Printf("GenerateBlock Failed")
			} else {
				log.Printf("Generated %d blocks", len(this_blocks))
			}

			for _, each_block := range this_blocks {
				log.Printf("this_block " + GetBlockHashString(each_block.GetBlockData()))
				this_hash_str := GetBlockHashString(each_block.GetBlockData())
				if err := client.PutBlock(&each_block, blockStoreAddr, &suc.Flag); err != nil {
					log.Printf("upload block failed, an error occured")
					log.Fatal(err)
				} else {
					// log.Printf("Successfully upload, blockstore now have")
					log.Printf(this_hash_str, string(each_block.GetBlockSize()))
				}

			}
			new_version := int32(1)
			// The problem making code
			err = client.UpdateFile(meta_data, &new_version)
			if err != nil {
				log.Printf(err.Error())
			}

		} else {
			//Server has a file with same name, check the version
			if meta_data.Version <= remote_meta_data.Version {
				//The server has a higher or equal version, sync down in previous but, don't upload
				continue
			} else {
				log.Printf("Upload renewal File from local, Local_V: " + strconv.Itoa(int(meta_data.Version)) +
					" Server_V: " + strconv.Itoa(int(remote_meta_data.Version)))
				//Local V > Server V, need to upload and update this file
				if meta_data.BlockHashList[0] == "0" {
					//Upload a Tombstone to the server
					log.Printf("Upload a Tombstone for: " + file_name)
					// new_version := meta_data.GetVersion() + 1
					//Already plused in local map parsing method
					new_version := meta_data.GetVersion()
					client.UpdateFile(meta_data, &new_version)
					continue
				}
				//Have unsynced file, upload it
				this_blocks, err := GenerateBlock(ConcatPath(client.BaseDir, file_name), client.BlockSize)
				if err != nil {
					log.Printf("GenerateBlock Failed")
				} else {
					log.Printf("get %d blocks", len(this_blocks))
				}
				meta_data.BlockHashList = meta_data.BlockHashList[:0]
				for _, each_block := range this_blocks {
					log.Printf("this_block " + GetBlockHashString(each_block.GetBlockData()))
					this_hash_str := GetBlockHashString(each_block.GetBlockData())
					if err := client.PutBlock(&each_block, blockStoreAddr, &suc.Flag); err != nil {
						log.Printf("upload block failed, an error occured")
						log.Fatal(err)
					} else {
						// log.Printf("Successfully upload, blockstore now have")
						// log.Printf(this_hash_str, string(each_block.GetBlockSize()))
					}
					meta_data.BlockHashList = append(meta_data.BlockHashList, this_hash_str)
				}

				new_version := meta_data.GetVersion()
				client.UpdateFile(meta_data, &new_version)

			}

		}
	}

	//Update the index.txt (pass)
	var a_new_map map[string]*FileMetaData
	client.GetFileInfoMap(&a_new_map)
	PrintMetaMap(a_new_map)
	err = WriteMetaFile(a_new_map, client.BaseDir)
	if err != nil {
		log.Printf("Fail to update index.txt")
		log.Fatal(err)
	} else {
		// log.Printf("Meta Server After:")
		// client.GetFileInfoMap(&remoteInfoMap.FileInfoMap)
		// PrintMetaMap(remoteInfoMap.FileInfoMap)
	}

}
