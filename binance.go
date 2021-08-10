package main

import (
	"os"
	"log"
	"fmt"
	"context"
	"time"
	"strconv"
	"math"
	"sync"
	"gonum.org/v1/gonum/stat"
	"github.com/sajari/regression"
	"github.com/adam-hanna/adf"
	"github.com/adshao/go-binance/v2"	
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/bson"
)
type TradeState struct {
	Asset1 string
	Asset2 string
	TradeAsset string
	Balance float64
	PrevBuy float64
	Position int
	Intercept float64
	Beta float64
}	
	
	
type Asset struct {
	Name string
	StartTime int64
	EndTime int64
	SumPrice float64
	Count int
	mu sync.Mutex
}
var (
	mgclient *mongo.Client
	mydb = "CointTrade"
	CollectionPrices = "Prices"
	CollectionState = "TradeState"
	CollectionPL = "PL"
	AssetList  []Asset
	MyState TradeState
	MyStateMu sync.Mutex
	InTrade = false
)
func GetAssetID(sym string) int {
	if sym == MyState.Asset1 {
		return 0
	} else if sym == MyState.Asset2 {
		return 1
	} else if sym == MyState.TradeAsset {
		return 2
	} else {
		return -1
	}
}
func checkNaN(x []float64) bool {
	for _,v := range x {
		if math.IsNaN(v) {
			return true
		}
	}
	return false
}
func rateOfReturn(prices []float64) []float64  {
	n := len(prices)
	//fmt.Println("array len",n)
	ror := make([]float64,n-1)
	for i,v := range prices {
		if i < n-1 {
			ror[i] = (v - prices[i+1])/prices[i+1]
		}
	}
	if checkNaN(ror) {
		fmt.Println("Error contain NaN",ror)
	}
	return ror
}
func GetBinanceClient() *binance.Client {
        apiKey := os.Getenv("Binance_API_KEY")
        secretKey := os.Getenv("Binance_SECRET_KEY")
        return binance.NewClient(apiKey, secretKey)
}
func getCurrentPrice(client *binance.Client, sym string) (float64,float64) {
        prices, priceerr := client.NewListBookTickersService().Symbol(sym).Do(context.Background())
        if priceerr != nil {
            fmt.Println(priceerr)
            return -1,-1
        }
        bid,converr1 := strconv.ParseFloat(prices[0].BidPrice,64)
        ask,converr2 := strconv.ParseFloat(prices[0].AskPrice,64)
        if converr1 != nil || converr2 != nil {
            fmt.Println(converr1,converr2)
            return -1,-1
        }
        return ask,bid
}
func execOrder(sig int) float64 {
        client := GetBinanceClient()
        ask,bid := getCurrentPrice(client,MyState.TradeAsset)
        var PL float64
        fmt.Println("current ",ask,bid)
	MyStateMu.Lock()
        if MyState.Position == 0 && sig == 1 {
                MyState.Position = 1
                MyState.PrevBuy = bid
                MyState.Balance = MyState.Balance - bid
                PL = 0
		saveState(MyState )
		savePL(MyState.TradeAsset, PL )
        } else if MyState.Position == 1 && sig == -1 {
                MyState.Position = 0
                MyState.Balance = MyState.Balance + ask
                PL = ask - MyState.PrevBuy
		saveState(MyState )
		savePL(MyState.TradeAsset, PL )
        }
	MyStateMu.Unlock()
        return PL
}
func sumPL(sym string) float64  {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coll := mgclient.Database(mydb).Collection(CollectionPL)
	opts := options.Find()
	opts.SetSort(bson.D{{"Timestamp", -1}})
	sortCursor, err := coll.Find(ctx, bson.D{{"Symbol",sym }}, opts)
	if err != nil {
	    log.Fatal(err)
	}
	var PLs []bson.M
	if err = sortCursor.All(ctx, &PLs); err != nil {
	    fmt.Println(err)
	}
	var totalPL float64 = 0
	for _,p := range PLs {
		switch v := p["PL"].(type) {
		case float64:
			totalPL += v
		case string:
			f,_ := strconv.ParseFloat(v,64)	
			totalPL += f
		case int:
			totalPL += float64(v)
		default:
			fmt.Println("unknown close type")
		}
		//fmt.Println(prices[i])
	
	}
	return totalPL	
}
func searchPrices(sym string, n int64) []float64  {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coll := mgclient.Database(mydb).Collection(CollectionPrices)
	opts := options.Find()
	opts.SetSort(bson.D{{"StartTime", -1}})
	opts.SetLimit(n)
	sortCursor, err := coll.Find(ctx, bson.D{{"Symbol",sym }}, opts)
	if err != nil {
	    log.Fatal(err)
	}
	var closePrices []bson.M
	if err = sortCursor.All(ctx, &closePrices); err != nil {
	    log.Fatal(err)
	}
	//fmt.Println(len(closePrices))
	prices := make([]float64,len(closePrices) )
	for i,cl := range closePrices {
		//fmt.Println(i,cl["StartTime"],cl["Close"])
		switch v := cl["Close"].(type) {
		case float64:
			prices[i] = v
		case string:
			f,_ := strconv.ParseFloat(v,64)	
			prices[i] = f
		case int:
			prices[i] = float64(v)
		default:
			fmt.Println("unknown close type")
		}
		//fmt.Println(prices[i])
	
	}
	return prices	
}
func getSignal(ys, xs, p0, p1  []float64, lookback int, beta float64, intercept float64) int {
	N := len(ys)
        res := make([]float64, N-1)
        for k := 1; k < N   ; k++ {
            res[k-1] = ys[k] - intercept - beta * xs[k]
        }
        current_res := ys[0] - intercept - beta * xs[0]
        //fmt.Println("current_res", current_res)
        sd := stat.Variance(res, nil)
        sd = math.Sqrt(sd)
        //fmt.Println("signal",current_res,sd)
	mid := int(N/2)
        ymean_long := stat.Mean(p0[1:mid],nil)
        ymean_short := stat.Mean(p0[mid:N],nil)
        xmean_long := stat.Mean(p1[1:mid],nil)
        xmean_short := stat.Mean(p1[mid:N],nil)

        ymm := ymean_short - ymean_long
        xmm := xmean_short - xmean_long

        thd := 0.5
        if current_res >= thd*sd {
            if ymm <= 0 || xmm >= 0 {
                return -1
            }
        }
        if current_res <= -1.0*thd*sd {
            if ymm >= 0 || xmm <= 0 {
                return 1
            }
        }
	return 0
}
func doTrade() {
	if InTrade {
		return
	}
	InTrade = true
	var lookback int64 = 15
	s1 := searchPrices(MyState.Asset1,lookback+2)
	r1 := rateOfReturn(s1)
	s2 := searchPrices(MyState.Asset2,lookback+2)
	r2 := rateOfReturn(s2)
	if len(r1) >= int(lookback) && len(r2) >= int(lookback) {
		beta := MyState.Beta
		intercept := MyState.Intercept
		sig := getSignal(r1,r2,s1,s2, int(lookback), beta, intercept)
		fmt.Println("signal",sig)
		PL := execOrder(sig)
		fmt.Println(MyState,PL)
		//houseKeepPL(mystate, 10)
	}
	InTrade = false
}
func runADF(x []float64) bool {
        a := adf.New(x,-3.45,0)
        a.Run()
        //fmt.Println("adf",a.Statistic)
        return a.IsStationary()
}
func runLR(ys []float64,xs []float64) (float64,float64,[]float64) {
        n := len(ys)
        fmt.Println("LR len",len(ys),len(xs) )
        res := make([]float64, n)
        r := new(regression.Regression)
        r.SetObserved("asset0 return")
        r.SetVar(0, "Asset1 return")
        for i,y := range ys {
            r.Train(regression.DataPoint(y, []float64{xs[i]}))
        }
        r.Run()
        //fmt.Println("Regression", r.GetCoeffs())
        for i,y := range ys {
            res[i] = y - r.Coeff(0) - r.Coeff(1)*xs[i]
        }
        return r.Coeff(0), r.Coeff(1), res
}
func getRMS(res []float64) float64 {
        rms := 0.0
        for _,e := range res {
           rms = rms + e*e
        }
        return math.Sqrt(rms/float64(len(res)))
}
func MinInt(a,b int) int {
	if a < b {
		return a
	}
	return b
}
func tuneParm() {
	var lookback int64 = 24*60
	s1 := searchPrices(MyState.Asset1,lookback+2)
	r1 := rateOfReturn(s1)
	s2 := searchPrices(MyState.Asset2,lookback+2)
	r2 := rateOfReturn(s2)
	n := MinInt(len(r1),len(r2))
	if len(r1) >= 10 && len(r2) >= 10 {
		intercept,beta,res := runLR(r1[0:n],r2[0:n])
		isCoint := runADF(res)
		fmt.Println("Parms:",intercept,beta,isCoint,getRMS(res))
		MyStateMu.Lock()
		MyState.Intercept = intercept	
		MyState.Beta = beta	
		MyStateMu.Unlock()
		saveState(MyState )
	}

}
func wsKlineHandler(event *binance.WsKlineEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coll := mgclient.Database(mydb).Collection(CollectionPrices)
	k := event.Kline
	//fmt.Println(k.StartTime,k.EndTime,k.Symbol,k.Close,k.IsFinal)
	i := GetAssetID(k.Symbol)
	AssetList[i].mu.Lock()
	tempAsset := AssetList[i]
	tempAsset.StartTime = k.StartTime
	tempAsset.EndTime = k.EndTime
	f,_ := strconv.ParseFloat(k.Close,64)	
	tempAsset.SumPrice += f
	tempAsset.Count += 1
	AssetList[i] = tempAsset
	if k.IsFinal {
		//fmt.Println("insert ", AssetList[i])
		results, err := coll.InsertOne(ctx, bson.D{
		    {Key: "StartTime", Value: k.StartTime},
		    {Key: "EndTime", Value: k.EndTime},
		    {Key: "Symbol", Value: k.Symbol},
		    {Key: "Close", Value: tempAsset.SumPrice/float64(tempAsset.Count) },
		})
		fmt.Println(results,err)
		tempAsset := AssetList[i]
		tempAsset.StartTime = k.StartTime
		tempAsset.EndTime = k.EndTime
		tempAsset.SumPrice = 0
		tempAsset.Count = 0
		AssetList[i] = tempAsset
		go doTrade()
	}
	AssetList[i].mu.Unlock()
}
func errHandler(err error) {
	fmt.Println(err)
}

func createMongoClient() *mongo.Client {
        agent := os.Getenv("MONGO_USER")
        passwd := os.Getenv("MONGO_PW")
        fqdn := os.Getenv("MONGO_HOST")
	clientOptions := options.Client().
		ApplyURI("mongodb+srv://"+agent+":" + passwd + 
"@" + fqdn + "/" + mydb + "?retryWrites=true&w=majority")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mgclient, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
	    log.Fatal(err)
	}
	return mgclient
}
func savePL(sym string, PL float64) bool{
	now := time.Now()
	tm := now.Unix()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coll := mgclient.Database(mydb).Collection(CollectionPL)
	results, err := coll.InsertOne(ctx, bson.D{
		    {Key: "Timestamp", Value: tm},
		    {Key: "Symbol", Value: sym},
		    {Key: "PL", Value: PL},
		})
	if err != nil {
		fmt.Println("cannot save PL", err)
		return false
	}
	fmt.Println(results,err)
	total := sumPL(sym)
	fmt.Println("total PL",sym,total)
	return true
}
func saveState(mystate TradeState ) bool{
	coll := mgclient.Database(mydb).Collection(CollectionState)
	opts := options.Replace().SetUpsert(true)
	filter := bson.D{
	    {"$and", bson.A{
		bson.D{{"Asset1", mystate.Asset1}},
		bson.D{{"Asset2", mystate.Asset2}},
	    }},
	}
	replacement := bson.M{
		"Asset1": mystate.Asset1,
		"Asset2": mystate.Asset2,
		"TradeAsset": mystate.TradeAsset,
		"Balance": mystate.Balance,
		"PrevBuy": mystate.PrevBuy,
		"Position": mystate.Position,
		"Intercept": mystate.Intercept,
		"Beta": mystate.Beta,
	}
	result, err := coll.ReplaceOne(context.TODO(), filter, replacement, opts)
	if err != nil {
		fmt.Println("cannot save state",err)
		return false
	}

	if result.MatchedCount != 0 {
		fmt.Println("matched and replaced an existing document")
		return true
	}
	if result.UpsertedCount != 0 {
		fmt.Printf("inserted a new document with ID %v\n", result.UpsertedID)
		return true
	}
	return true
}
func loadState(s1 string, s2 string, s3 string) TradeState {
	var mystate TradeState
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	coll := mgclient.Database(mydb).Collection(CollectionState)
	filter := bson.D{
	    {"$and", bson.A{
		bson.D{{"Asset1", s1}},
		bson.D{{"Asset2", s2}},
	    }},
	}
	err := coll.FindOne(ctx, filter).Decode(&mystate)
	if err == mongo.ErrNoDocuments {
	    fmt.Println("record does not exist")
	    mystate = TradeState{s1,s2,s3,0,0,0,0,0}
	} else if err != nil {
	    log.Fatal(err)
	}
	return mystate
}
func RegisterStream(sym string) (chan struct{}, chan struct{}) {
	AssetList[GetAssetID(sym)] = Asset{sym,0,0,0,0,sync.Mutex{}}
	doneC, stopC, err := binance.WsKlineServe(sym, "1m", wsKlineHandler, errHandler)
	if err != nil {
	    fmt.Println(sym)
	    fmt.Println(err)
	}
	return doneC,stopC
}
func main() {
	mgclient = createMongoClient()	
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	mgclient.Ping(ctx,readpref.Primary())
	//MyState = TradeState{"ETHUSDT","BTCUSDT","ETHBTC",0,0,0,0,0,sync.Mutex{}}
	MyState = loadState("ETHUSDT","BTCUSDT","ETHBTC")
	tuneParm()	
	AssetList = make([]Asset,3)
	doneC1,_ := RegisterStream(MyState.Asset1)
	doneC2,_ := RegisterStream(MyState.Asset2)
	doneC3,_ := RegisterStream(MyState.TradeAsset)
	<-doneC1
	<-doneC2
	<-doneC3
}

