package parsers

import (
	"fmt"
	"sync"
	"os"
	"log"
	"strconv"
	"strings"
	"runtime"
	"time"
	"github.com/forchain/ethereumbigdata/lib"
	"net/http"
	"github.com/forchain/ethrpc"
	"math/big"
	"io/ioutil"
	"encoding/json"
)

const (
	REDUCE_BLOCK_NO = 4370000

	ETH_UNIT = 1e18
)

type tAccount struct {
	balance float64
	time    uint32
}

type tAccountMap map[string]tAccount

type tBalanceChange struct {
	addr   string
	change float64
	time   int
}

type tRewardFee struct {
	reward float64
	fee    float64
}

type BalanceParser struct {
	rpc_ *ethrpc.EthRPC

	maxBlock_ int

	fileNO_ int
	outDir_ string

	accountMap_ tAccountMap

	reduceNum_ uint32
	reduceSum_ float64

	reduceNum2010_ uint32
	reduceSum2010_ float64

	reduceNum2014_ uint32
	reduceSum2014_ float64

	unspentMapLock_ *sync.RWMutex
	balanceMapLock_ *sync.RWMutex

	balanceChangeCh_ chan *tBalanceChange
	balanceReadyCh_  chan bool

	cpuNum_   int
	blockCh_  chan *ethrpc.Block
	blockMap_ map[int]*ethrpc.Block

	fileList_ []int
	blockNum_ uint32

	blockNO_     int
	sumReward_   float64
	sumFee_      float64
	rewardFeeCh_ chan *tRewardFee

	unit_    *big.Int
	genesis_ tGenesis
}

func (_b *BalanceParser) loadBlock(_blockNO int, _wg *sync.WaitGroup) {
	defer _wg.Done()

	block, err := _b.rpc_.EthGetBlockByNumber(_blockNO, true)
	if err != nil {
		log.Fatalln("loadBlock EthGetBlockByNumber", err)
	}

	_b.blockCh_ <- block
}

type tGenesis map[string]map[string]string

func (_b *BalanceParser) loadGenesis() {
	pwd, _ := os.Getwd()
	raw, err := ioutil.ReadFile(pwd + "/data/genesis_block.json")
	if err != nil {
		log.Fatalln("LoadGenesis ReadFile", err)
	}
	_b.genesis_ = make(tGenesis)
	err = json.Unmarshal(raw, &_b.genesis_)
	if err != nil {
		log.Fatalln("LoadGenesis Unmarshal", err)
	}

	for addr, balance := range _b.genesis_ {
		wei := new(big.Float)
		wei.SetString(balance["wei"])
		eth := new(big.Float)
		eth.Quo(wei, big.NewFloat(1e18))

		fEth, _ := eth.Float64()
		_b.accountMap_["0x"+addr] = tAccount{fEth, 0}
	}
	log.Println("loadGenesis", len(_b.accountMap_))
}

func (_b *BalanceParser) Init(_rpc string, _out string) {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 10000

	_b.rpc_ = ethrpc.NewEthRPC("http://" + _rpc)

	_b.outDir_ = _out
	os.RemoveAll(_out)
	os.Mkdir(_out, os.ModePerm)

	_b.cpuNum_ = runtime.NumCPU()
	_b.blockCh_ = make(chan *ethrpc.Block, _b.cpuNum_)
	_b.blockMap_ = make(map[int]*ethrpc.Block)
	_b.accountMap_ = make(tAccountMap)

	_b.balanceChangeCh_ = make(chan *tBalanceChange, _b.cpuNum_)
	_b.balanceReadyCh_ = make(chan bool)
	_b.rewardFeeCh_ = make(chan *tRewardFee, _b.cpuNum_)

	_b.unit_ = new(big.Int)
	_b.unit_.SetUint64(ETH_UNIT)

	_b.loadGenesis()

	var err error
	if num, err := _b.rpc_.EthBlockNumber(); err != nil || num == 0 {
		if s, err := _b.rpc_.EthSyncing(); err == nil && s != nil {
			_b.maxBlock_ = s.CurrentBlock
		}
	} else {
		_b.maxBlock_ = num
	}

	if err != nil {
		log.Fatal(err)
	}
	if _b.maxBlock_ == 0 {
		_b.maxBlock_ = 5000000
	}
	log.Println("[MAX]", _b.maxBlock_)
}

func (_b *BalanceParser) Parse(_rpc string, _out string) {
	_b.Init(_rpc, _out)

	wgBlock := new(sync.WaitGroup)
	go _b.processBlock(wgBlock)

	go _b.processBalance(nil)

	go _b.processFee()
	wg := new(sync.WaitGroup)
	for i := 0; i < _b.maxBlock_; i++ {
		wg.Add(1)
		go _b.loadBlock(i, wg)
		if (i+1)%_b.cpuNum_ == 0 {
			wg.Wait()
		}
	}
	wg.Wait()
	wgBlock.Wait()
}

type tSortedBalance []string

func (s tSortedBalance) Len() int {
	return len(s)
}
func (s tSortedBalance) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s tSortedBalance) Less(i, j int) bool {
	// remove trailing return
	t1 := strings.Split(s[i][:len(s[i])-1], " ")
	t2 := strings.Split(s[j][:len(s[j])-1], " ")
	if len(t1) == 2 && len(t2) == 2 {
		if v1, err := strconv.ParseUint(t1[1], 10, 0); err == nil {
			if v2, err := strconv.ParseUint(t2[1], 10, 0); err == nil {
				return v1 > v2
			}
		}
	}

	return len(s[i]) < len(s[j])
}

func (_b *BalanceParser) saveDayReport(_days uint32) {
	fileName := fmt.Sprintf("%v/reduce.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln("saveDayReport OpenFile", err)
	}
	defer f.Close()
	line := fmt.Sprintf("%v,%v,%v\n", _days, _b.reduceNum_, _b.reduceSum_)
	f.WriteString(line)

	fileName2010 := fmt.Sprintf("%v/reduce2010.csv", _b.outDir_)
	f2010, err := os.OpenFile(fileName2010, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f2010.Close()
	line2010 := fmt.Sprintf("%v,%v,%v\n", _days, _b.reduceNum2010_, _b.reduceSum2010_)
	f2010.WriteString(line2010)

	fileName2014 := fmt.Sprintf("%v/reduce2014.csv", _b.outDir_)
	f2014, err := os.OpenFile(fileName2014, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f2014.Close()
	line2014 := fmt.Sprintf("%v,%v,%v\n", _days, _b.reduceNum2014_, _b.reduceSum2014_)
	f2014.WriteString(line2014)

	log.Println("[REDUCE]", line, line2010, line2014)
}

func (_b *BalanceParser) saveMonthReport(_blockTime time.Time) {
	lastDate := _blockTime.Add(-time.Hour * 24)

	fileName := fmt.Sprintf("%v/balance.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln("saveMonthReport OpenFile", err)
	}
	defer f.Close()

	fileName100 := fmt.Sprintf("%v/balance100.csv", _b.outDir_)
	f100, err := os.OpenFile(fileName100, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f100.Close()

	fileName1000 := fmt.Sprintf("%v/balance1000.csv", _b.outDir_)
	f1000, err := os.OpenFile(fileName1000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f1000.Close()

	fileName10000 := fmt.Sprintf("%v/balance10000.csv", _b.outDir_)
	f10000, err := os.OpenFile(fileName10000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f10000.Close()

	fileName100000 := fmt.Sprintf("%v/balance100000.csv", _b.outDir_)
	f100000, err := os.OpenFile(fileName100000, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer f100000.Close()

	topList := new(lib.TopList)
	topList.Init(100000)
	balanceNum := len(_b.accountMap_)
	balanceSum := 0.0
	for _, v := range _b.accountMap_ {
		balanceSum += v.balance
		topList.Push(v.balance)
	}
	line := fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), balanceNum, balanceSum)
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	}
	log.Println("[ALL]", line)

	top := topList.Sorted()
	sum := 0.0
	for k, v := range top {
		sum += v
		if k == 99 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f100.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[100]", line)
		} else if k == 999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f1000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[1000]", line)
		} else if k == 9999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
			if _, err = f10000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[10000]", line)
		}
	}
	if len(top) >= 100000 {
		line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, float64(sum)/float64(balanceSum))
		if _, err = f100000.WriteString(line); err != nil {
			log.Fatalln(err, line)
		}
		log.Println("[100000]", line)
	}

	fileNameReward := fmt.Sprintf("%v/reward.csv", _b.outDir_)
	fReward, err := os.OpenFile(fileNameReward, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer fReward.Close()

	line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), _b.sumReward_, _b.sumFee_)
	if _, err = fReward.WriteString(line); err != nil {
		log.Fatalln(err, line)
	}
	log.Println("[REWARD]", line)
}

func (_b *BalanceParser) processFee() {
	for change := range _b.rewardFeeCh_ {
		_b.sumReward_ += change.fee
		_b.sumFee_ += change.fee
	}
}

func (_b *BalanceParser) processBalance(_blockTime *time.Time) {
	for change := range _b.balanceChangeCh_ {
		account, ok := _b.accountMap_[change.addr]
		if !ok {
			account.time = uint32(change.time)
		}

		balance := account.balance
		if balance >= 1000 && change.change < 0 && _blockTime != nil {
			delta := time.Now().Sub(*_blockTime)
			days := uint32(delta.Hours() / 24)
			if days <= 720 {
				_b.reduceNum_ += 1
				_b.reduceSum_ += -change.change

				t := time.Unix(int64(account.time), 0)
				if t.Year() <= 2010 {
					_b.reduceNum2010_ += 1
					_b.reduceSum2010_ += -change.change

				} else if t.Year() <= 2014 {
					_b.reduceNum2014_ += 1
					_b.reduceSum2014_ += -change.change
				}
			}
		}

		if balance += change.change; balance > 0 {
			account.balance = balance
			_b.accountMap_[change.addr] = account
		} else if balance == 0 {
			delete(_b.accountMap_, change.addr)
		}
	}

	_b.balanceReadyCh_ <- true
}

func convertPrice(_price *big.Int) float64 {
	ether := new(big.Float)
	price := new(big.Float).SetInt(_price)
	unit := new(big.Float).SetUint64(ETH_UNIT)
	ether.Quo(price, unit)

	ret, _ := ether.Float64()
	return ret
}

func (_b *BalanceParser) processBlock(_wg *sync.WaitGroup) {
	defer _wg.Done()

	lastLogTime := new(time.Time)
	for {
		if block, ok := _b.blockMap_[_b.blockNO_]; ok {
			reward := 0.0
			if _b.blockNO_ >= REDUCE_BLOCK_NO {
				reward = 3
			} else {
				reward = 5
			}

			fee := 0.0
			for _, t := range block.Transactions {
				gasPrice := convertPrice(&t.GasPrice)
				txFee := gasPrice * float64(t.Gas)
				fee += txFee

				val := convertPrice(&t.Value)

				_b.balanceChangeCh_ <- &tBalanceChange{t.From, -val - txFee, block.Timestamp}

				_b.balanceChangeCh_ <- &tBalanceChange{t.To, val, block.Timestamp}
			}

			_b.rewardFeeCh_ <- &tRewardFee{reward, fee}

			_b.balanceChangeCh_ <- &tBalanceChange{block.Miner, fee + reward, block.Timestamp}

			blockTime := time.Unix(int64(block.Timestamp), 0)
			if blockTime.Day() != lastLogTime.Day() && !lastLogTime.IsZero() {
				close(_b.balanceChangeCh_)
				<-_b.balanceReadyCh_

				delta := time.Now().Sub(blockTime)
				if days := uint32(delta.Hours() / 24); days < 720 {
					_b.saveDayReport(days)
					_b.reduceNum_ = 0
					_b.reduceSum_ = 0
					_b.reduceNum2010_ = 0
					_b.reduceSum2010_ = 0
					_b.reduceNum2014_ = 0
					_b.reduceSum2014_ = 0
				}

				if blockTime.Month() != lastLogTime.Month() {
					_b.saveMonthReport(blockTime)
					_b.sumFee_ = 0
					_b.sumReward_ = 0
				}
				lastLogTime = &blockTime

				_b.balanceChangeCh_ = make(chan *tBalanceChange)
				go _b.processBalance(&blockTime)
			}

			delete(_b.blockMap_, _b.blockNO_)

			_b.blockNO_++
		} else {
			block, ok := <-_b.blockCh_
			if !ok {
				break
			}

			_b.blockMap_[block.Number] = block
		}
	}
}
