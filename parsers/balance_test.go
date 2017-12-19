package parsers

import (
	"testing"
	"os"
	"io/ioutil"
	"encoding/json"
	"log"
	"math/big"
	"fmt"
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

func recoverTest() {
	throw()
}

func throw() {
	panic("underflow")
}

func TestRecover(t *testing.T) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()

	recoverTest()
}

func TestUnderflow(t *testing.T) {

	big1 := big.NewInt(-6470220429140951616)
	big2, _ := big.NewInt(0).SetString("13605734051180781568", 10)

	big3 := big.NewInt(0)
	big3.Add(big1, big2)

	t.Log(big3.String())

	big1 = big.NewInt(-996414840377181072)
	big2, _ = big.NewInt(0).SetString("3479865757200000000", 10)

	big3 = big.NewInt(0)
	big3.Add(big1, big2)

	t.Log(big3.String())

	//underflow - 6470220429140951616
	//13605734051180781568
}
