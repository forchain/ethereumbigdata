package parsers

import (
	"testing"
	"os"
	"io/ioutil"
	"encoding/json"
	"log"
	"math/big"
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

func TestBalanceParser_loadGenesisBigInt(t *testing.T) {
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
	sum := big.NewInt(0)
	for _, balance := range genesis {
		bi := big.NewInt(0)
		bi.SetString(balance["wei"], 10)
		sum.Add(sum, bi)
	}
	// Genesis (60M Crowdsale+12M Other):	72,009,990.50 Ether
	log.Println("loadGenesis", sum.String())
	log.Print(len(genesis))

}