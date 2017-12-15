package parsers

import (
	"testing"
	"os"
	"io/ioutil"
	"encoding/json"
	"log"
)

func TestBalanceParser_loadGenesis(t *testing.T) {
	pwd, _ := os.Getwd()
	log.Println(pwd)
	raw, err := ioutil.ReadFile(pwd + "/../data/genesis_block.json")
	if err != nil {
		log.Fatalln("LoadGenesis ReadFile", err)
	}
	genesis := make(tGenesis)
	err = json.Unmarshal(raw, &genesis)
	if err != nil {
		t.Fatal(err)
	}
	log.Print(len(genesis))
}