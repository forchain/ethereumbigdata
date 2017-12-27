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
	"sort"
)

const (
	REDUCE_BLOCK_NO = 4370000

	ETH_UNIT = 1e18
)

type tBalanceChange struct {
	addr      string
	change    float64
	timestamp int
}

type tRewardFee struct {
	reward float64
	fee    float64
}

type tAccount struct {
	balance   float64
	timestamp int
}

type tBlockInfo struct {
	block  *ethrpc.Block
	uncles []*ethrpc.Block
}

type BalanceParser struct {
	rpc_ *ethrpc.EthRPC

	maxBlock_ int

	fileNO_ int
	outDir_ string

	balanceMap_ map[string]tAccount

	reduceNum_ uint32
	reduceSum_ float64

	reduceNumICO_ uint32
	reduceSumICO_ float64

	balanceChangeCh_ chan *tBalanceChange
	balanceReadyCh_  chan bool

	cpuNum_   int
	blockCh_  chan tBlockInfo
	blockMap_ map[int]tBlockInfo

	blockNO_     int
	sumReward_   float64
	sumFee_      float64
	rewardFeeCh_ chan *tRewardFee

	genesis_ tGenesis
}

func (_b *BalanceParser) loadBlock(_blockNO int, _wg *sync.WaitGroup) {
	defer _wg.Done()

	block, err := _b.rpc_.EthGetBlockByNumber(_blockNO, true)
	if err != nil {
		log.Fatalln("loadBlock EthGetBlockByNumber", err)
	}

	uncles := make([]*ethrpc.Block, 0)
	for k := range block.Uncles {
		uncle, err := _b.rpc_.EthGetUncleByBlockNumberAndIndex(_blockNO, k)
		if err != nil {
			log.Fatalln("loadBlock EthGetUncleByBlockNumberAndIndex", err)
		}
		uncles = append(uncles, uncle)
	}

	_b.blockCh_ <- tBlockInfo{block, uncles}
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

	sum := 0.0
	for addr, balance := range _b.genesis_ {
		wei, _ := big.NewFloat(0).SetString(balance["wei"])
		wei.Quo(wei, big.NewFloat(ETH_UNIT))
		eth, _ := wei.Float64()
		_b.balanceMap_["0x"+addr] = tAccount{eth, 0}
		sum += eth
	}
	// Genesis (60M Crowdsale+12M Other):	72,009,990.50 Ether
	log.Println("loadGenesis", len(_b.balanceMap_), sum)
}

func (_b *BalanceParser) Init(_rpc string, _out string) {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 10000

	_b.rpc_ = ethrpc.NewEthRPC(_rpc)

	_b.outDir_ = _out
	os.RemoveAll(_out)
	os.Mkdir(_out, os.ModePerm)

	_b.cpuNum_ = runtime.NumCPU()
	_b.blockCh_ = make(chan tBlockInfo, _b.cpuNum_)
	_b.blockMap_ = make(map[int]tBlockInfo)
	_b.balanceMap_ = make(map[string]tAccount)

	_b.balanceChangeCh_ = make(chan *tBalanceChange, _b.cpuNum_)
	_b.balanceReadyCh_ = make(chan bool)
	_b.rewardFeeCh_ = make(chan *tRewardFee, _b.cpuNum_)

	_b.sumReward_ = 0.0
	_b.sumFee_ = 0.0
	_b.reduceSumICO_ = 0.0
	_b.reduceSum_ = 0.0

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

func (_b *BalanceParser) period() {
	periodMap := make(map[int]float64)

	for _, b := range _b.balanceMap_ {
		if b.timestamp == 0 {
			periodMap[-1] += b.balance
		} else {
			t := time.Unix(int64(b.timestamp), 0)
			delta := time.Now().Sub(t)
			hours := int(delta.Hours() / 24)
			periodMap[hours] += b.balance
		}
	}

	fileName := fmt.Sprintf("%v/period.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	size := len(periodMap) - 1
	for i := 0; i < size; i++ {
		line := fmt.Sprintf("%v,%v\n", i, periodMap[i])
		if _, err = f.WriteString(line); err != nil {
			log.Fatalln(err, line)
		} else {
			log.Print(line)
		}
	}

	line := fmt.Sprintf("%v,%v\n", size, periodMap[-1])
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}
}

func (_b *BalanceParser) hold() {
	balanceList := make(lib.Float64Sorted, 0)
	for _, v := range _b.balanceMap_ {
		balanceList = append(balanceList, v.balance)
	}
	sort.Sort(balanceList)

	divide := len(balanceList) / 1000
	n := 0
	sum := 0.0
	divideSum := 0.0
	sum1000, sum10000, sum100000 := 0.0, 0.0, 0.0
	divideKeys := make([]int, 0)
	divideVals := make([]float64, 0)
	for k, v := range balanceList {
		n = k + 1
		sum += v
		divideSum += v

		if n == 1000 {
			sum1000 = sum
		} else if n == 10000 {
			sum10000 = sum
		} else if n == 100000 {
			sum100000 = sum
		}

		if n%divide == 0 {
			divideKeys = append(divideKeys, n)
			divideVals = append(divideVals, divideSum)
			divideSum = 0
		}
	}

	fileName := fmt.Sprintf("%v/hold.csv", _b.outDir_)
	f, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}

	defer f.Close()

	line := fmt.Sprintf("1000,%v,%v\n", sum1000, float64(sum1000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}
	line = fmt.Sprintf("10000,%v,%v\n", sum10000, float64(sum10000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}
	line = fmt.Sprintf("100000,%v,%v\n", sum100000, float64(sum100000)/float64(sum))
	if _, err = f.WriteString(line); err != nil {
		log.Fatalln(err, line)
	} else {
		log.Print(line)
	}

	for k, key := range divideKeys {
		val := divideVals[k]
		line = fmt.Sprintf("%v,%v,%v\n", key, val, float64(val)/float64(sum))
		if _, err = f.WriteString(line); err != nil {
			log.Fatalln(err, line)
		} else {
			log.Print(line)
		}
	}

	log.Println("[HOLD]", fileName)
}

func (_b *BalanceParser) Parse(_rpc string, _out string) {
	_b.Init(_rpc, _out)

	wgBlock := new(sync.WaitGroup)
	wgBlock.Add(1)
	go _b.processBlock(wgBlock)

	go _b.processBalance()

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
	close(_b.blockCh_)
	wgBlock.Wait()

	<-_b.balanceReadyCh_

	_b.hold()
	_b.period()
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

	fileNameICO := fmt.Sprintf("%v/reduceICO.csv", _b.outDir_)
	fICO, err := os.OpenFile(fileNameICO, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModeAppend|os.ModePerm)
	if err != nil {
		log.Fatalln(err)
	}
	defer fICO.Close()
	lineICO := fmt.Sprintf("%v,%v,%v\n", _days, _b.reduceNumICO_, _b.reduceSumICO_)
	fICO.WriteString(lineICO)

	log.Println("[REDUCE]", line, lineICO)
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
	balanceNum := len(_b.balanceMap_)
	balanceSum := 0.0
	for _, v := range _b.balanceMap_ {
		if v.balance > 0 {
			topList.Push(v.balance)
			balanceSum += v.balance
		} else {
			balanceNum -= 1
		}
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
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, sum/balanceSum)
			if _, err = f100.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[100]", line)
		} else if k == 999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, sum/balanceSum)
			if _, err = f1000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[1000]", line)
		} else if k == 9999 {
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, sum/balanceSum)
			if _, err = f10000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[10000]", line)
		}
	}
	if len(top) >= 100000 {
		line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum, sum/balanceSum)
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
		_b.sumReward_ += change.reward
		_b.sumFee_ += change.fee
	}
}

func (_b *BalanceParser) processBalance() {
	for change := range _b.balanceChangeCh_ {
		account, _ := _b.balanceMap_[change.addr]
		account.timestamp = change.timestamp
		if account.balance >= 10000 && change.change < 0 {
			_b.reduceNum_ += 1
			_b.reduceSum_ -= change.change

			// crowd sale
			addr := change.addr[2:]
			if _, ok := _b.genesis_[addr]; ok {
				_b.reduceNumICO_ += 1
				_b.reduceSumICO_ -= change.change
			}
		}

		account.balance += change.change
		if account.balance == 0 {
			delete(_b.balanceMap_, change.addr)
		} else {
			_b.balanceMap_[change.addr] = account
		}
	}

	_b.balanceReadyCh_ <- true
}

func (_b *BalanceParser) processBlock(_wg *sync.WaitGroup) {
	defer _wg.Done()

	lastLogTime := new(time.Time)
	for {
		if info, ok := _b.blockMap_[_b.blockNO_]; ok {
			block := info.block
			reward := 0.0
			if _b.blockNO_ >= REDUCE_BLOCK_NO {
				reward = 3
			} else {
				reward = 5
			}

			uncleReward := 0.0
			for _, uncle := range info.uncles {
				r := float64(uncle.Number+8-block.Number) * reward / 8
				_b.balanceChangeCh_ <- &tBalanceChange{uncle.Miner, r, block.Timestamp}

				uncleReward += reward / 32
			}

			fee := 0.0
			for _, t := range block.Transactions {
				txFee := big.NewFloat(0).Mul(big.NewFloat(float64(t.Gas)), big.NewFloat(0).SetInt(&t.GasPrice))
				txFee.Quo(big.NewFloat(0).Set(txFee), big.NewFloat(ETH_UNIT))
				fee, _ = txFee.Float64()

				val := big.NewFloat(0).Quo(big.NewFloat(0).SetInt(&t.Value), big.NewFloat(ETH_UNIT))

				from, _ := big.NewFloat(0).Add(val, txFee).Float64()
				to, _ := val.Float64()

				_b.balanceChangeCh_ <- &tBalanceChange{t.From, -from, block.Timestamp}

				_b.balanceChangeCh_ <- &tBalanceChange{t.To, to, block.Timestamp}
			}

			_b.rewardFeeCh_ <- &tRewardFee{reward, fee}

			_b.balanceChangeCh_ <- &tBalanceChange{block.Miner, fee + reward + uncleReward, block.Timestamp}

			blockTime := time.Unix(int64(block.Timestamp), 0)
			if blockTime.Day() != lastLogTime.Day() && _b.blockNO_ > 1 {
				close(_b.balanceChangeCh_)
				<-_b.balanceReadyCh_

				delta := time.Now().Sub(blockTime)
				days := uint32(delta.Hours() / 24)
				_b.saveDayReport(days)
				_b.reduceNum_ = 0
				_b.reduceSum_ = 0
				_b.reduceNumICO_ = 0
				_b.reduceSumICO_ = 0

				if blockTime.Month() != lastLogTime.Month() {
					_b.saveMonthReport(blockTime)
					_b.sumFee_ = 0
					_b.sumReward_ = 0
				}

				_b.balanceChangeCh_ = make(chan *tBalanceChange)
				go _b.processBalance()
			}

			lastLogTime = &blockTime

			delete(_b.blockMap_, _b.blockNO_)

			_b.blockNO_++
		} else {
			info, ok := <-_b.blockCh_
			if !ok {
				break
			}

			_b.blockMap_[info.block.Number] = info
		}
	}
	close(_b.balanceChangeCh_)
}
