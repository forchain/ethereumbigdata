package main

import (
	"github.com/forchain/ethereumbigdata/parsers"
	"flag"
	"log"
)

func main() {
	out := new(string)
	rpc := new(string)

	flag.StringVar(rpc, "rpc", "127.0.0.1:8545", "RPC server address")
	flag.StringVar(out, "out", "/tmp/out", "Out path")
	flag.Parse()

	args := flag.Args()
	if len(args) == 0 {
		log.Println("-out OUT_PATH -rpc RPC_SERVER <balance>")
		return
	}

	if args[0] == "balance" {
		p := new(parsers.BalanceParser)
		p.Parse(*rpc, *out)
	}
}
