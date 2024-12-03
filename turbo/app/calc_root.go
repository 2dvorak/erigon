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
	"fmt"

	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/commitment"
	"github.com/erigontech/erigon-lib/common/length"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/node"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/trie"
)

var calcRootCommand = cli.Command{
	Action:    MigrateFlags(calcRoot),
	Name:      "calcroot",
	Usage:     "Calculate the trie root of the database",
	ArgsUsage: "<key>",
	Flags: []cli.Flag{
		&utils.DataDirFlag,
	},
}

// dbGet will get a value fromat genesis file and writes it as
// the zero'd block (i.e. genesis) or will fail hard if it can't succeed.
func calcRoot(cliCtx *cli.Context) error {
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

	return tryHexPatriciaTrie(cliCtx, chaindb)

	return nil
}

func tryHexPatriciaTrie(cliCtx *cli.Context, tx kv.RwTx, toTxNum uint64) error {
	domains, err := state.NewSharedDomains(tx, log.New())
	if err != nil {
		return err
	}
	defer domains.Close()
	domains.SetTxNum(toTxNum)
	s := state.NewSharedDomainsCommitmentContext(domains, commitment.ModeDirect, commitment.VariantHexPatriciaTrie)

	hph := commitment.NewHexPatriciaHashed(length.Addr, s, "")

	rootHash, err := hph.Process(cliCtx.Context, s.Updates(), "calcroot")
	if err != nil {
		return err
	}
	fmt.Printf("rootHash=0x%x\n", rootHash)
	return nil
}

func tryFlatKVLoader(cliCtx *cli.Context, chaindb kv.RwDB) error {
	rd := trie.NewRetainList(0)
	hc := func(keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		fmt.Printf("keyHex=0x%x, hasState=%d, hasTree=%d, hasHash=%d, hashes=0x%x, rootHash=0x%x\n", keyHex, hasState, hasTree, hasHash, hashes, rootHash)
		return nil
	}
	shc := func(accWithInc []byte, keyHex []byte, hasState, hasTree, hasHash uint16, hashes, rootHash []byte) error {
		fmt.Printf("accWithInc=0x%x, keyHex=0x%x, hasState=%d, hasTree=%d, hasHash=%d, hashes=0x%x, rootHash=0x%x\n", accWithInc, keyHex, hasState, hasTree, hasHash, hashes, rootHash)
		return nil
	}
	loader := trie.NewFlatDBTrieLoader("calcroot", rd, hc, shc, true)
	chaindb.View(cliCtx.Context, func(tx kv.Tx) error {
		res, err := loader.CalcTrieRoot(tx, nil)
		if err != nil {
			return err
		}
		fmt.Printf("root=0x%x\n", res)
		return nil
	})
}
