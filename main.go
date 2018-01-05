package main

import (
	"github.com/Newtrong/ethereumbigdata/parsers"
	"flag"
)

func main() {
	out := new(string)
	rpc := new(string)

	flag.StringVar(rpc, "rpc", "http://127.0.0.1:8545", "RPC server address")
	flag.StringVar(out, "out", "/tmp/out", "Out path")
	flag.Parse()

	p := new(parsers.BalanceParser)
	p.Parse(*rpc, *out)
}
