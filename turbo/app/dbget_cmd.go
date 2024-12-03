// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package app

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/turbo/debug"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/node"
)

var dbGetCommand = cli.Command{
	Action:    MigrateFlags(dbGet),
	Name:      "dbget",
	Usage:     "Get a value from the database",
	ArgsUsage: "<key>",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
	},
}

func dbGetOne(cliCtx *cli.Context) error {
	var logger log.Logger
	var err error
	if logger, _, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	dbname := cliCtx.Args().First()
	if len(dbname) == 0 {
		utils.Fatalf("Must supply dbname")
	}
	key := cliCtx.Args().Get(1)
	if len(key) == 0 {
		utils.Fatalf("Must supply key")
	}
	var keybuf []byte
	if len(key) > 2 && key[:2] == "0x" {
		keybuf, err = hex.DecodeString(key[2:])
		if err != nil {
			utils.Fatalf("Failed to decode hex: %v", err)
		}
	} else {
		keybuf, err = hex.DecodeString(key)
		if err != nil {
			utils.Fatalf("Failed to decode hex: %v", err)
		}
	}

	stack := MakeConfigNodeDefault(cliCtx, logger)
	defer stack.Close()

	chaindb, err := node.OpenDatabase(cliCtx.Context, stack.Config(), kv.ChainDB, "", false, logger)
	if err != nil {
		utils.Fatalf("Failed to open database: %v", err)
	}
	defer chaindb.Close()

	chaindb.View(cliCtx.Context, func(tx kv.Tx) error {
		if dbname == "MaxTxNum" {
			val, err := tx.GetOne(dbname, keybuf)
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			fmt.Printf("0x%x\n", val)
		} else if dbname == "AccountHistoryVals" {
			prefix := cliCtx.Args().Get(2)
			var prefixbuf []byte
			if len(prefix) > 0 {
				prefixbuf, err = hex.DecodeString(prefix)
				if err != nil {
					utils.Fatalf("Failed to decode hex: %v", err)
				}
			}
			stream, err := tx.RangeDupSort(dbname, keybuf, prefixbuf, nil, true, -1)
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			for stream.HasNext() {
				k, v, err := stream.Next()
				if err != nil {
					utils.Fatalf("Failed to get value: %v", err)
				}
				fmt.Printf("key=0x%x, val=0x%x\n", k, v)
			}
		} else if dbname == "AccountVals" {
			val, err := tx.GetOne(dbname, keybuf)
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			fmt.Printf("key=0x%x, val=0x%x\n", keybuf, val)
		} else if dbname == "AccountHistoryKeys" {
			stream, err := tx.RangeDupSort(dbname, keybuf, nil, nil, true, -1)
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			for stream.HasNext() {
				k, v, err := stream.Next()
				if err != nil {
					utils.Fatalf("Failed to get acc: %v", err)
				}
				fmt.Printf("key=0x%x, val=0x%x\n", k, v)
			}
		}
		return nil
	})
	return nil
}

// dbGet will get a value fromat genesis file and writes it as
// the zero'd block (i.e. genesis) or will fail hard if it can't succeed.
func dbGet(cliCtx *cli.Context) error {
	//return dbGetOne(cliCtx)
	var logger log.Logger
	var err error
	if logger, _, _, err = debug.Setup(cliCtx, true /* rootLogger */); err != nil {
		return err
	}
	// Make sure we have a valid genesis JSON
	dbname := cliCtx.Args().First()
	if len(dbname) == 0 {
		utils.Fatalf("Must supply dbname")
	}
	key := cliCtx.Args().Get(1)
	if len(key) == 0 {
		utils.Fatalf("Must supply key")
	}

	// Open and initialise both full and light databases
	stack := MakeConfigNodeDefault(cliCtx, logger)
	defer stack.Close()

	chaindb, err := node.OpenDatabase(cliCtx.Context, stack.Config(), kv.ChainDB, "", false, logger)
	if err != nil {
		utils.Fatalf("Failed to open database: %v", err)
	}
	defer chaindb.Close()

	var table string
	_ = table
	var keybuf []byte
	switch dbname {
	case "header_canonical", "headercanonical", "headercan", "hc":
		table = kv.HeaderCanonical
		if len(key) > 2 && key[:2] == "0x" {
			keybuf, err = hex.DecodeString(key[2:])
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
		} else {
			number, err := strconv.ParseUint(key, 10, 64)
			if err != nil {
				utils.Fatalf("Failed to parse number: %v", err)
			}
			keybuf = make([]byte, 8)
			binary.BigEndian.PutUint64(keybuf, number)
			fmt.Printf("keybuf: %x\n", keybuf)
		}
	case "headers", "head", "h":
		table = kv.Headers
		if len(key) > 2 && key[:2] == "0x" {
			keybuf, err = hex.DecodeString(key[2:])
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
		} else {
			keybuf, err = hex.DecodeString(key)
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
		}
	case "block_body", "blockbody", "bb":
		table = kv.BlockBody
	case "account":
		table = kv.PlainState
		keybuf, err = hex.DecodeString(key)
		if err != nil {
			utils.Fatalf("Failed to decode hex: %v", err)
		}
	case "dump":
	default:
		if len(key) > 2 && key[:2] == "0x" {
			keybuf, err = hex.DecodeString(key[2:])
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
		} else {
			keybuf, err = hex.DecodeString(key)
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
		}
	}

	// CanonicalHash(0)  block_num_u64 -> hash
	// key 0x0000000000000000
	// val 0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3
	// val, err := tx.GetOne(kv.HeaderCanonical, keybuf)
	// ken dbget hc 0

	// Header(0, 0xcf...)  block_num_u64 + hash -> rlp(header)
	// key 0x0000000000000000d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3
	// val 0xf9....
	// rlp decode `ken dbget header 0x0000000000000000d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3` | jq -r ".[2]"
	// stateRoot 23a9977c16397aa93fc8caf303abdf054e92adb2b99c5f35b89af6566d1a8cd0

	// towards account address 0x854ca8508c8be2bb1f3c244045786410cb7d5d0a
	// echo '854ca8508c8be2bb1f3c244045786410cb7d5d0a' | xxd -r -p | keccak-256sum
	// secure account address b408da3d631c0b1216ba459576b64c83dbb43f1d120f1f99b253cf8f03fd7343
	//
	// TrieNode(hash)  hash -> node
	//
	// key 0x23a9977c16397aa93fc8caf303abdf054e92adb2b99c5f35b89af6566d1a8cd0
	// val extension node[0xb] = a809f9a29d68fe866e414e4736330e1a598f63818adb822d2ef4a8e6991d43ab
	//
	// key 0xa809f9a29d68fe866e414e4736330e1a598f63818adb822d2ef4a8e6991d43ab
	// val extension node[0x4] = 34fbba9b7b56524bccbef3172898af3b25bdc991db80131fe54ad042ec52e45e
	//
	// key 0x34fbba9b7b56524bccbef3172898af3b25bdc991db80131fe54ad042ec52e45e
	// val 01ce80893635c9adc5dea000008001c0 = 0x01 (EOA type) + 0xce80893635c9adc5dea000008001c0 (rlp(account))
	//     = ["","3635c9adc5dea00000","","01",[]] = [nonce, balance, humanReadable, keytype, key]

	// keyBytes = append(append([]byte("h"), common.Int64ToByteBigEndian(0)...), []byte("n")...)
	if dbname == "dumpall" {
		all := chaindb.AllTables()

		for table, _ := range all {
			fmt.Printf("table %s\n", table)
			chaindb.View(cliCtx.Context, func(tx kv.Tx) error {
				stream, err := tx.Range(table, nil, nil)
				if err != nil {
					utils.Fatalf("Failed to get acc: %v", err)
				}
				for stream.HasNext() {
					k, v, err := stream.Next()
					if err != nil {
						utils.Fatalf("Failed to get acc: %v", err)
					}
					fmt.Printf("key=0x%-80x, val=0x%-80x\n", k, v)
				}
				return nil
			})
		}
		return nil
	} else if dbname == "dump" {
		fmt.Printf("dump %s\n", key)
		chaindb.View(cliCtx.Context, func(tx kv.Tx) error {
			stream, err := tx.Range(key, nil, nil)
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			for stream.HasNext() {
				k, v, err := stream.Next()
				if err != nil {
					utils.Fatalf("Failed to get acc: %v", err)
				}
				fmt.Printf("%s key=0x%-80x, val=0x%-80x\n", key, k, v)
			}
			return nil
		})
		return nil
	}

	chaindb.View(cliCtx.Context, func(tx kv.Tx) error {
		var val []byte
		// interate all values for all keys
		/*stream, err := tx.Range(kv.Headers, nil, nil)
		if err != nil {
			utils.Fatalf("Failed to get acc: %v", err)
		}
		for stream.HasNext() {
			k, v, err := stream.Next()
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			fmt.Printf("key=0x%-80x, val=0x%-80x\n", k, v)
		}
		return nil*/

		// get one value
		/*val, err = tx.GetOne(dbname, keybuf)
		if err != nil {
			utils.Fatalf("Failed to get acc: %v", err)
		}
		fmt.Printf("0x%x\n", val)
		return nil*/

		// iterate all values with the same key
		err = tx.ForEach(dbname, keybuf, func(k, v []byte) error {
			if bytes.Equal(k, keybuf) {
				fmt.Printf("key=0x%-80x, val=0x%-80x\n", k, v)
			} else {
				return errors.New("not equal")
			}
			return nil
		})

		fmt.Printf("range dup sort\n")
		fromPrefix, err := hex.DecodeString("0000000000000194")
		toPrefix, err := hex.DecodeString("0000000000000195")
		stream, err := tx.RangeDupSort(dbname, keybuf, fromPrefix, toPrefix, true, -1)
		if err != nil {
			utils.Fatalf("Failed to get acc: %v", err)
		}
		for stream.HasNext() {
			k, v, err := stream.Next()
			if err != nil {
				utils.Fatalf("Failed to get acc: %v", err)
			}
			fmt.Printf("key=0x%-80x, val=0x%-80x\n", k, v)
		}
		return nil
		/*
			val, err := tx.GetOne(table, keybuf)

			hashBuf, err := hex.DecodeString("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3")
			if err != nil {
				utils.Fatalf("Failed to decode hex: %v", err)
			}
			fmt.Printf("0x%x\n", val)

			hash := common.Hash(hashBuf)
			headerRlp := rawdb.ReadHeaderRLP(tx, hash, 0)
			if err != nil {
				utils.Fatalf("Failed to get value: %v", err)
			}
			fmt.Printf("0x%x\n", headerRlp)

			headerkey := dbutils.HeaderKey(0, hash)
			val, err = tx.GetOne("Header", headerkey)
			if err != nil {
				utils.Fatalf("Failed to get value: %v", err)
			}
			fmt.Printf("0x%x\n", val)
			val, err = tx.GetOne("BlockBody", headerkey)
			if err != nil {
				utils.Fatalf("Failed to get value: %v", err)
			}
			fmt.Printf("0x%x\n", val)

			val, err = tx.GetOne("Receipt", keybuf)
			if err != nil {
				utils.Fatalf("Failed to get receipt: %v", err)
			}
			fmt.Printf("0x%x\n", val)*/

		//accBuf, err := hex.DecodeString("415c8893D514F9BC5211d36eEDA4183226b84AA7")
		//accBuf, err := hex.DecodeString("05a56E2D52c817161883f50c441c3228CFe54d9f")
		//accBuf, err := hex.DecodeString("05a56E2D52c817161883f50c441c3228CFe54d9f" + "0000000000000003")
		accBuf, err := hex.DecodeString("05a56E2D52c817161883f50c441c3228CFe54d9f" + "0000000000000005")
		if err != nil {
			utils.Fatalf("Failed to decode hex: %v", err)
		}
		val, err = tx.GetOne(kv.PlainState, accBuf)
		if err != nil {
			utils.Fatalf("Failed to get acc: %v", err)
		}
		fmt.Printf("plainstate 0x%x\n", val)

		//addr :=
		//reader := state.NewPlainStateReader(tx)
		//reader.ReadAccountData

		accHashBuf, err := hex.DecodeString("4be8251692195afc818c92b485fcb8a4691af89cbe5a2ab557b83a4261be2a9a")
		if err != nil {
			utils.Fatalf("Failed to decode hex: %v", err)
		}
		val, err = tx.GetOne("PlainState", accHashBuf)
		if err != nil {
			utils.Fatalf("Failed to get acc: %v", err)
		}
		fmt.Printf("0x%x\n", val)
		return nil
	})

	return nil
}
