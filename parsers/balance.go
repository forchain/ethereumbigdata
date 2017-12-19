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

type tBalanceChange struct {
	addr   string
	change *big.Int
}

type tRewardFee struct {
	reward *big.Int
	fee    *big.Int
}

type BalanceParser struct {
	rpc_ *ethrpc.EthRPC

	maxBlock_ int

	fileNO_ int
	outDir_ string

	balanceMap_ map[string]*big.Int

	reduceNum_ uint32
	reduceSum_ *big.Int

	reduceNumICO_ uint32
	reduceSumICO_ *big.Int

	balanceChangeCh_ chan *tBalanceChange
	balanceReadyCh_  chan bool

	cpuNum_   int
	blockCh_  chan *ethrpc.Block
	blockMap_ map[int]*ethrpc.Block

	blockNO_     int
	sumReward_   *big.Int
	sumFee_      *big.Int
	rewardFeeCh_ chan *tRewardFee

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

	sum := big.NewInt(0)
	for addr, balance := range _b.genesis_ {
		wei := big.NewInt(0)
		wei.SetString(balance["wei"], 10)
		_b.balanceMap_["0x"+addr] = wei
		sum.Add(sum, wei)
	}
	// Genesis (60M Crowdsale+12M Other):	72,009,990.50 Ether
	log.Println("loadGenesis", len(_b.balanceMap_), sum.String())
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
	_b.balanceMap_ = make(map[string]*big.Int)

	_b.balanceChangeCh_ = make(chan *tBalanceChange, _b.cpuNum_)
	_b.balanceReadyCh_ = make(chan bool)
	_b.rewardFeeCh_ = make(chan *tRewardFee, _b.cpuNum_)

	_b.sumReward_ = big.NewInt(0)
	_b.sumFee_ = big.NewInt(0)
	_b.reduceSumICO_ = big.NewInt(0)
	_b.reduceSum_ = big.NewInt(0)

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
	balanceSum := big.NewInt(0)
	for _, v := range _b.balanceMap_ {
		if v.Sign() > 0 {
			balanceSum.Add(big.NewInt(0).Set(balanceSum), v)
			topList.Push(v)
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
	sum := big.NewInt(0)
	for k, v := range top {
		sum.Add(sum, v)
		if k == 99 {
			fSum := big.NewFloat(0).SetInt(sum)
			fBalance := big.NewFloat(0).SetInt(balanceSum)
			quo := big.NewFloat(0).Quo(fSum, fBalance)
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum.String(), quo.String())
			if _, err = f100.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[100]", line)
		} else if k == 999 {
			fSum := big.NewFloat(0).SetInt(sum)
			fBalance := big.NewFloat(0).SetInt(balanceSum)
			quo := big.NewFloat(0).Quo(fSum, fBalance)
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum.String(), quo.String())
			if _, err = f1000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[1000]", line)
		} else if k == 9999 {
			fSum := big.NewFloat(0).SetInt(sum)
			fBalance := big.NewFloat(0).SetInt(balanceSum)
			quo := big.NewFloat(0).Quo(fSum, fBalance)
			line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum.String(), quo.String())
			if _, err = f10000.WriteString(line); err != nil {
				log.Fatalln(err, line)
			}
			log.Println("[10000]", line)
		}
	}
	if len(top) >= 100000 {
		fSum := big.NewFloat(0).SetInt(sum)
		fBalance := big.NewFloat(0).SetInt(balanceSum)
		quo := big.NewFloat(0).Quo(fSum, fBalance)

		line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), sum.String(), quo.String())
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

	line = fmt.Sprintf("%v,%v,%v\n", lastDate.Local().Format("2006-01-02"), _b.sumReward_.String(), _b.sumFee_.String())
	if _, err = fReward.WriteString(line); err != nil {
		log.Fatalln(err, line)
	}
	log.Println("[REWARD]", line)
}

func (_b *BalanceParser) processFee() {
	for change := range _b.rewardFeeCh_ {
		_b.sumReward_.Add(_b.sumReward_, change.fee)
		_b.sumFee_.Add(_b.sumFee_, change.fee)
	}
}

func (_b *BalanceParser) processBalance(_blockTime *time.Time) {
	debugBalance := big.NewInt(0)
	debugChange := big.NewInt(0)

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err, debugBalance.String(), debugChange.String())
		}
	}()


	for change := range _b.balanceChangeCh_ {
		balance, ok := _b.balanceMap_[change.addr]
		if !ok {
			balance = big.NewInt(0)
			_b.balanceMap_[change.addr] = balance
		}

		debugChange = change.change
		debugBalance = balance

		rich := big.NewInt(0)
		rich.Mul(big.NewInt(ETH_UNIT), big.NewInt(10000))

		if balance.Cmp(rich) >= 0 && change.change.Sign() < 0 && _blockTime != nil {
			delta := time.Now().Sub(*_blockTime)
			days := uint32(delta.Hours() / 24)
			if days <= 720 {
				_b.reduceNum_ += 1

				_b.reduceSum_.Set(big.NewInt(0).Sub(_b.reduceSum_, change.change))

				// crowd sale
				if _, ok := _b.genesis_[change.addr]; ok {
					_b.reduceNumICO_ += 1
					_b.reduceSumICO_.Set(big.NewInt(0).Sub(_b.reduceSumICO_, change.change))
				}
			}
		}

		//// GOLANG BUG?
		//if balance.Sign() < 0 && change.change.Sign() > 0 {
		//	absBalance := big.NewInt(0).Abs(balance)
		//	if absBalance.Cmp(change.change) > 0 {
		//		balance.Neg(big.NewInt(0).Sub(absBalance, change.change))
		//	} else {
		//		b := big.NewInt(0)
		//		b.Add(balance, change.change)
		//		balance.Set(b)
		//	}
		//} else {
		//	b := big.NewInt(0)
		//	b.Add(balance, change.change)
		//	balance.Set(b)
		//}

		newBalance := big.NewInt(0)
		newBalance.Add(balance, change.change)

		balance.Set(newBalance)

		if balance.Sign() == 0 {
			delete(_b.balanceMap_, change.addr)
		}
	}

	_b.balanceReadyCh_ <- true
}

func (_b *BalanceParser) processBlock(_wg *sync.WaitGroup) {
	defer _wg.Done()

	lastLogTime := new(time.Time)
	for {
		if block, ok := _b.blockMap_[_b.blockNO_]; ok {
			reward := big.NewInt(0)
			if _b.blockNO_ >= REDUCE_BLOCK_NO {
				reward.SetUint64(3 * ETH_UNIT)
			} else {
				reward.SetUint64(5 * ETH_UNIT)
			}

			fee := big.NewInt(0)
			for _, t := range block.Transactions {
				txFee := big.NewInt(0)
				txFee.Mul(&t.GasPrice, big.NewInt(int64(t.Gas)))

				fee.Add(big.NewInt(0).Set(fee), txFee)

				fromVal := big.NewInt(0).Add(&t.Value, txFee)

				_b.balanceChangeCh_ <- &tBalanceChange{t.From, fromVal.Neg(fromVal)}

				_b.balanceChangeCh_ <- &tBalanceChange{t.To, &t.Value}
			}

			_b.rewardFeeCh_ <- &tRewardFee{reward, fee}

			blockVal := big.NewInt(0).Add(fee, reward)

			_b.balanceChangeCh_ <- &tBalanceChange{block.Miner, blockVal}

			blockTime := time.Unix(int64(block.Timestamp), 0)
			if blockTime.Day() != lastLogTime.Day() && _b.blockNO_ > 1 {
				close(_b.balanceChangeCh_)
				<-_b.balanceReadyCh_

				delta := time.Now().Sub(blockTime)
				if days := uint32(delta.Hours() / 24); days < 720 {
					_b.saveDayReport(days)
					_b.reduceNum_ = 0
					_b.reduceSum_.SetUint64(0)
					_b.reduceNumICO_ = 0
					_b.reduceSumICO_.SetUint64(0)
				}

				if blockTime.Month() != lastLogTime.Month() {
					_b.saveMonthReport(blockTime)
					_b.sumFee_.SetUint64(0)
					_b.sumReward_.SetUint64(0)
				}

				_b.balanceChangeCh_ = make(chan *tBalanceChange)
				go _b.processBalance(&blockTime)
			}

			lastLogTime = &blockTime

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
