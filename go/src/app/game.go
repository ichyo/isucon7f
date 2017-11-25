package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"math/big"
	"os"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gorilla/websocket"
	"github.com/jmoiron/sqlx"
)

type AddingCache struct {
	que   map[string]map[int64]*big.Int
	total map[string]*big.Int
	mux   *sync.Mutex
}
type DumpCache struct {
	que   map[string]map[int64]string
	total map[string]string
}

var (
	ac = newAddingCache()
)

func newAddingCache() *AddingCache {
	d := &AddingCache{
		make(map[string]map[int64]*big.Int),
		make(map[string]*big.Int),
		&sync.Mutex{},
	}
	d.ParseFile()
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-t.C:
				d.DumpFile()
			}
		}
	}()
	return d
}

func (c *AddingCache) Clean() {
	c.que = make(map[string]map[int64]*big.Int)
	c.total = make(map[string]*big.Int)
}

func (c *AddingCache) ParseFile() {
	c.Clean()
	queFile, err := os.Open("/home/isucon/que.csv")
	defer queFile.Close()
	if err == nil {
		r := csv.NewReader(queFile)
		records, err := r.ReadAll()
		if err == nil {
			for _, r := range records {
				name := r[0]
				time, _ := strconv.ParseInt(r[1], 10, 64)
				val := str2big(r[2])
				if _, ok := c.que[name]; !ok {
					c.que[name] = make(map[int64]*big.Int)
				}
				c.que[name][time] = val
			}
		}
	}

	totalFile, err := os.Open("/home/isucon/total.csv")
	defer totalFile.Close()
	if err == nil {
		r := csv.NewReader(totalFile)
		records, err := r.ReadAll()
		if err == nil {
			for _, r := range records {
				name := r[0]
				val := str2big(r[1])
				c.total[name] = val
			}
		}
	}
}

func (c *AddingCache) DumpFile() {
	log.Println("dumpfile")
	queFile, err := os.Create("/home/isucon/que.csv")
	defer queFile.Close()
	if err != nil {
		log.Println("failed to dump")
		return
	}
	totalFile, err := os.Create("/home/isucon/total.csv")
	defer totalFile.Close()
	if err != nil {
		log.Println("failed to dump")
		return
	}

	queWriter := csv.NewWriter(queFile)
	for name, v := range c.que {
		for time, val := range v {
			log.Println(name, strconv.FormatInt(time, 10), val.String())
			err := queWriter.Write([]string{name, strconv.FormatInt(time, 10), val.String()})
			if err != nil {
				log.Println("Error: " + err.Error())
			}
		}
	}
	queWriter.Flush()
	if err := queWriter.Error(); err != nil {
		log.Println("Error: " + err.Error())
	}

	totalWriter := csv.NewWriter(totalFile)
	for name, val := range c.total {
		log.Println(name, val)
		err := totalWriter.Write([]string{name, val.String()})
		if err != nil {
			log.Println("Error: " + err.Error())
		}
	}
	totalWriter.Flush()
	if err := totalWriter.Error(); err != nil {
		log.Println("Error: " + err.Error())
	}
}

func (c *AddingCache) addIsu(roomName string, reqIsu big.Int, reqTime int64) bool {
	c.mux.Lock()
	defer c.mux.Unlock()
	if _, ok := c.que[roomName]; !ok {
		c.que[roomName] = make(map[int64]*big.Int)
	}
	if _, ok := c.que[roomName][reqTime]; !ok {
		c.que[roomName][reqTime] = big.NewInt(0)
	}
	c.que[roomName][reqTime].Add(c.que[roomName][reqTime], &reqIsu)
	return true
}

func (c *AddingCache) getTotal(roomName string, reqTime int64) big.Int {
	c.mux.Lock()
	defer c.mux.Unlock()
	if _, ok := c.total[roomName]; !ok {
		c.total[roomName] = big.NewInt(0)
	}
	if _, ok := c.que[roomName]; !ok {
		c.que[roomName] = make(map[int64]*big.Int)
	}
	vs := make([]int64, 0, 0)
	rest := new(big.Int)
	for k, v := range c.que[roomName] {
		if k <= reqTime-1000 {
			c.total[roomName].Add(c.total[roomName], big.NewInt(0).Mul(v, big.NewInt(1000)))
			vs = append(vs, k)
		} else if k <= reqTime {
			rest.Add(rest, big.NewInt(0).Mul(v, big.NewInt(1000)))
		}
	}
	for _, k := range vs {
		delete(c.que[roomName], k)
	}
	rest.Add(rest, c.total[roomName])
	return *rest
}
func (c *AddingCache) setAddingAt(roomName string, currentTime int64, addingAt map[int64]Adding) {
	c.mux.Lock()
	defer c.mux.Unlock()
	for k, v := range c.que[roomName] {
		if k <= currentTime {
			// 存在しないはず
		} else {
			addingAt[k] = Adding{roomName, k, v.String(), v}
		}
	}
}

var (
	roomTime = map[string]int64{}
	timeMux  = &sync.Mutex{}
)

func getCurrentTime() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

func printError(err error) {
	log.Println("Error:" + err.Error())
}

func updateRoomTime(tx *sqlx.Tx, roomName string, reqTime int64) (int64, bool) {
	timeMux.Lock()
	defer timeMux.Unlock()
	currentTime := getCurrentTime()
	if rt, ok := roomTime[roomName]; ok {
		if currentTime < rt {
			log.Println("room time is future")
			return 0, false
		}
	}
	if reqTime != 0 {
		if reqTime < currentTime {
			log.Println("reqTime is past")
			return 0, false
		}
	}
	roomTime[roomName] = currentTime
	return currentTime, true
}

type GameRequest struct {
	RequestID int    `json:"request_id"`
	Action    string `json:"action"`
	Time      int64  `json:"time"`

	// for addIsu
	Isu string `json:"isu"`

	// for buyItem
	ItemID      int `json:"item_id"`
	CountBought int `json:"count_bought"`
}

type GameResponse struct {
	RequestID int  `json:"request_id"`
	IsSuccess bool `json:"is_success"`
}

// 10進数の指数表記に使うデータ。JSONでは [仮数部, 指数部] という2要素配列になる。
type Exponential struct {
	// Mantissa * 10 ^ Exponent
	Mantissa int64
	Exponent int64
}

func (n Exponential) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("[%d,%d]", n.Mantissa, n.Exponent)), nil
}

type Adding struct {
	RoomName string   `json:"-" db:"room_name"`
	Time     int64    `json:"time" db:"time"`
	Isu      string   `json:"isu" db:"isu"`
	IsuVal   *big.Int `json:"-" db"-"`
}

type Buying struct {
	RoomName string `db:"room_name"`
	ItemID   int    `db:"item_id"`
	Ordinal  int    `db:"ordinal"`
	Time     int64  `db:"time"`
}

type Schedule struct {
	Time       int64       `json:"time"`
	MilliIsu   Exponential `json:"milli_isu"`
	TotalPower Exponential `json:"total_power"`
}

type Item struct {
	ItemID      int         `json:"item_id"`
	CountBought int         `json:"count_bought"`
	CountBuilt  int         `json:"count_built"`
	NextPrice   Exponential `json:"next_price"`
	Power       Exponential `json:"power"`
	Building    []Building  `json:"building"`
}

type OnSale struct {
	ItemID int   `json:"item_id"`
	Time   int64 `json:"time"`
}

type Building struct {
	Time       int64       `json:"time"`
	CountBuilt int         `json:"count_built"`
	Power      Exponential `json:"power"`
}

type GameStatus struct {
	Time     int64      `json:"time"`
	Adding   []Adding   `json:"adding"`
	Schedule []Schedule `json:"schedule"`
	Items    []Item     `json:"items"`
	OnSale   []OnSale   `json:"on_sale"`
}

type mItem struct {
	ItemID int   `db:"item_id"`
	Power1 int64 `db:"power1"`
	Power2 int64 `db:"power2"`
	Power3 int64 `db:"power3"`
	Power4 int64 `db:"power4"`
	Price1 int64 `db:"price1"`
	Price2 int64 `db:"price2"`
	Price3 int64 `db:"price3"`
	Price4 int64 `db:"price4"`
}

var mItems = map[int]mItem{
	1:  {ItemID: 1, Power1: 0, Power2: 1, Power3: 0, Power4: 1, Price1: 0, Price2: 1, Price3: 1, Price4: 1},
	2:  {ItemID: 2, Power1: 0, Power2: 1, Power3: 1, Power4: 1, Price1: 0, Price2: 1, Price3: 2, Price4: 1},
	3:  {ItemID: 3, Power1: 1, Power2: 10, Power3: 0, Power4: 2, Price1: 1, Price2: 3, Price3: 1, Price4: 2},
	4:  {ItemID: 4, Power1: 1, Power2: 24, Power3: 1, Power4: 2, Price1: 1, Price2: 10, Price3: 0, Price4: 3},
	5:  {ItemID: 5, Power1: 1, Power2: 25, Power3: 100, Power4: 3, Price1: 2, Price2: 20, Price3: 20, Price4: 2},
	6:  {ItemID: 6, Power1: 1, Power2: 30, Power3: 147, Power4: 13, Price1: 1, Price2: 22, Price3: 69, Price4: 17},
	7:  {ItemID: 7, Power1: 5, Power2: 80, Power3: 128, Power4: 6, Price1: 6, Price2: 61, Price3: 200, Price4: 5},
	8:  {ItemID: 8, Power1: 20, Power2: 340, Power3: 180, Power4: 3, Price1: 9, Price2: 105, Price3: 134, Price4: 14},
	9:  {ItemID: 9, Power1: 55, Power2: 520, Power3: 335, Power4: 5, Price1: 48, Price2: 243, Price3: 600, Price4: 7},
	10: {ItemID: 10, Power1: 157, Power2: 1071, Power3: 1700, Power4: 12, Price1: 157, Price2: 625, Price3: 1000, Price4: 13},
	11: {ItemID: 11, Power1: 2000, Power2: 7500, Power3: 2600, Power4: 3, Price1: 2001, Price2: 5430, Price3: 1000, Price4: 3},
	12: {ItemID: 12, Power1: 1000, Power2: 9000, Power3: 0, Power4: 17, Price1: 963, Price2: 7689, Price3: 1, Price4: 19},
	13: {ItemID: 13, Power1: 11000, Power2: 11000, Power3: 11000, Power4: 23, Price1: 10000, Price2: 2, Price3: 2, Price4: 29},
}

func (item *mItem) GetPower(count int) *big.Int {
	// power(x):=(cx+1)*d^(ax+b)
	a := item.Power1
	b := item.Power2
	c := item.Power3
	d := item.Power4
	x := int64(count)

	s := big.NewInt(c*x + 1)
	t := new(big.Int).Exp(big.NewInt(d), big.NewInt(a*x+b), nil)
	return new(big.Int).Mul(s, t)
}

func (item *mItem) GetPrice(count int) *big.Int {
	// price(x):=(cx+1)*d^(ax+b)
	a := item.Price1
	b := item.Price2
	c := item.Price3
	d := item.Price4
	x := int64(count)

	s := big.NewInt(c*x + 1)
	t := new(big.Int).Exp(big.NewInt(d), big.NewInt(a*x+b), nil)
	return new(big.Int).Mul(s, t)
}

func str2big(s string) *big.Int {
	x := new(big.Int)
	x.SetString(s, 10)
	return x
}

func big2exp(n *big.Int) Exponential {
	s := n.String()

	if len(s) <= 15 {
		return Exponential{n.Int64(), 0}
	}

	t, err := strconv.ParseInt(s[:15], 10, 64)
	if err != nil {
		log.Panic(err)
	}
	return Exponential{t, int64(len(s) - 15)}
}

func addIsu(roomName string, reqIsu *big.Int, reqTime int64) bool {
	_, ok := updateRoomTime(nil, roomName, reqTime)
	if !ok {
		log.Println("Warn: updateRoomTime failed")
		return false
	}

	return ac.addIsu(roomName, *reqIsu, reqTime)
}

func buyItem(roomName string, itemID int, countBought int, reqTime int64) bool {
	tx, err := db.Beginx()
	if err != nil {
		printError(err)
		return false
	}

	_, ok := updateRoomTime(tx, roomName, reqTime)
	if !ok {
		log.Println("Warn: updateRoomTime failed")
		return false
	}

	var countBuying int
	err = tx.Get(&countBuying, "SELECT COUNT(*) FROM buying WHERE room_name = ? AND item_id = ?", roomName, itemID)
	if err != nil {
		printError(err)
		tx.Rollback()
		return false
	}
	if countBuying != countBought {
		tx.Rollback()
		log.Println(roomName, itemID, countBought+1, " is already bought")
		return false
	}

	totalMilliIsu_ := ac.getTotal(roomName, reqTime)
	totalMilliIsu := new(big.Int)
	totalMilliIsu.Add(totalMilliIsu, &totalMilliIsu_) // TODO: oh

	var buyings []Buying
	err = tx.Select(&buyings, "SELECT item_id, ordinal, time FROM buying WHERE room_name = ?", roomName)
	if err != nil {
		printError(err)
		tx.Rollback()
		return false
	}
	for _, b := range buyings {
		item := mItems[b.ItemID]
		cost := new(big.Int).Mul(item.GetPrice(b.Ordinal), big.NewInt(1000))
		totalMilliIsu.Sub(totalMilliIsu, cost)
		if b.Time <= reqTime {
			gain := new(big.Int).Mul(item.GetPower(b.Ordinal), big.NewInt(reqTime-b.Time))
			totalMilliIsu.Add(totalMilliIsu, gain)
		}
	}

	item := mItems[itemID]
	need := new(big.Int).Mul(item.GetPrice(countBought+1), big.NewInt(1000))
	if totalMilliIsu.Cmp(need) < 0 {
		log.Println("not enough")
		tx.Rollback()
		return false
	}

	_, err = tx.Exec("INSERT INTO buying(room_name, item_id, ordinal, time) VALUES(?, ?, ?, ?)", roomName, itemID, countBought+1, reqTime)
	if err != nil {
		printError(err)
		tx.Rollback()
		return false
	}

	if err := tx.Commit(); err != nil {
		printError(err)
		return false
	}

	return true
}

func getStatus(roomName string) (*GameStatus, error) {
	tx, err := db.Beginx()
	if err != nil {
		return nil, err
	}

	currentTime, ok := updateRoomTime(tx, roomName, 0)
	if !ok {
		tx.Rollback()
		return nil, fmt.Errorf("updateRoomTime failure")
	}

	buyings := []Buying{}
	err = tx.Select(&buyings, "SELECT item_id, ordinal, time FROM buying WHERE room_name = ?", roomName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	status, err := calcStatus(roomName, currentTime, mItems, buyings)
	if err != nil {
		return nil, err
	}

	// calcStatusに時間がかかる可能性があるので タイムスタンプを取得し直す
	latestTime := getCurrentTime()

	status.Time = latestTime
	return status, err
}

func calcStatus(roomName string, currentTime int64, mItems map[int]mItem, buyings []Buying) (*GameStatus, error) {
	var (
		// 1ミリ秒に生産できる椅子の単位をミリ椅子とする
		totalMilliIsu_ = ac.getTotal(roomName, currentTime)
		totalPower     = big.NewInt(0)

		itemPower    = map[int]*big.Int{}    // ItemID => Power
		itemPrice    = map[int]*big.Int{}    // ItemID => Price
		itemOnSale   = map[int]int64{}       // ItemID => OnSale
		itemBuilt    = map[int]int{}         // ItemID => BuiltCount
		itemBought   = map[int]int{}         // ItemID => CountBought
		itemBuilding = map[int][]Building{}  // ItemID => Buildings
		itemPower0   = map[int]Exponential{} // ItemID => currentTime における Power
		itemBuilt0   = map[int]int{}         // ItemID => currentTime における BuiltCount

		addingAt = map[int64]Adding{}   // Time => currentTime より先の Adding
		buyingAt = map[int64][]Buying{} // Time => currentTime より先の Buying
	)
	totalMilliIsu := new(big.Int)
	totalMilliIsu.Add(totalMilliIsu, &totalMilliIsu_) // TODO: oh

	for itemID := range mItems {
		itemPower[itemID] = big.NewInt(0)
		itemBuilding[itemID] = []Building{}
	}

	ac.setAddingAt(roomName, currentTime, addingAt)

	for _, b := range buyings {
		// buying は 即座に isu を消費し buying.time からアイテムの効果を発揮する
		itemBought[b.ItemID]++
		m := mItems[b.ItemID]
		totalMilliIsu.Sub(totalMilliIsu, new(big.Int).Mul(m.GetPrice(b.Ordinal), big.NewInt(1000)))

		if b.Time <= currentTime {
			itemBuilt[b.ItemID]++
			power := m.GetPower(itemBought[b.ItemID])
			totalMilliIsu.Add(totalMilliIsu, new(big.Int).Mul(power, big.NewInt(currentTime-b.Time)))
			totalPower.Add(totalPower, power)
			itemPower[b.ItemID].Add(itemPower[b.ItemID], power)
		} else {
			buyingAt[b.Time] = append(buyingAt[b.Time], b)
		}
	}

	for _, m := range mItems {
		itemPower0[m.ItemID] = big2exp(itemPower[m.ItemID])
		itemBuilt0[m.ItemID] = itemBuilt[m.ItemID]
		price := m.GetPrice(itemBought[m.ItemID] + 1)
		itemPrice[m.ItemID] = price
		if 0 <= totalMilliIsu.Cmp(new(big.Int).Mul(price, big.NewInt(1000))) {
			itemOnSale[m.ItemID] = 0 // 0 は 時刻 currentTime で購入可能であることを表す
		}
	}

	schedule := []Schedule{
		Schedule{
			Time:       currentTime,
			MilliIsu:   big2exp(totalMilliIsu),
			TotalPower: big2exp(totalPower),
		},
	}

	// itemPrice * 1000 を先に計算する
	var itemPrice1000 = map[int]*big.Int{}
	for itemID := range mItems {
		itemPrice1000[itemID] = new(big.Int).Mul(itemPrice[itemID], big.NewInt(1000))
	}

	// currentTime から 1000 ミリ秒先までシミュレーションする
	for t := currentTime + 1; t <= currentTime+1000; t++ {
		totalMilliIsu.Add(totalMilliIsu, totalPower)
		updated := false

		// 時刻 t で発生する adding を計算する
		if a, ok := addingAt[t]; ok {
			updated = true
			totalMilliIsu.Add(totalMilliIsu, new(big.Int).Mul(a.IsuVal, big.NewInt(1000)))
		}

		// 時刻 t で発生する buying を計算する
		if _, ok := buyingAt[t]; ok {
			updated = true
			updatedID := map[int]bool{}
			for _, b := range buyingAt[t] {
				m := mItems[b.ItemID]
				updatedID[b.ItemID] = true
				itemBuilt[b.ItemID]++
				power := m.GetPower(b.Ordinal)
				itemPower[b.ItemID].Add(itemPower[b.ItemID], power)
				totalPower.Add(totalPower, power)
			}
			for id := range updatedID {
				itemBuilding[id] = append(itemBuilding[id], Building{
					Time:       t,
					CountBuilt: itemBuilt[id],
					Power:      big2exp(itemPower[id]),
				})
			}
		}

		if updated {
			schedule = append(schedule, Schedule{
				Time:       t,
				MilliIsu:   big2exp(totalMilliIsu),
				TotalPower: big2exp(totalPower),
			})
		}

		// 時刻 t で購入可能になったアイテムを記録する
		for itemID := range mItems {
			if _, ok := itemOnSale[itemID]; ok {
				continue
			}
			if 0 <= totalMilliIsu.Cmp(itemPrice1000[itemID]) {
				itemOnSale[itemID] = t
			}
		}
	}

	gsAdding := []Adding{}
	for _, a := range addingAt {
		gsAdding = append(gsAdding, a)
	}

	gsItems := []Item{}
	for itemID, _ := range mItems {
		gsItems = append(gsItems, Item{
			ItemID:      itemID,
			CountBought: itemBought[itemID],
			CountBuilt:  itemBuilt0[itemID],
			NextPrice:   big2exp(itemPrice[itemID]),
			Power:       itemPower0[itemID],
			Building:    itemBuilding[itemID],
		})
	}

	gsOnSale := []OnSale{}
	for itemID, t := range itemOnSale {
		gsOnSale = append(gsOnSale, OnSale{
			ItemID: itemID,
			Time:   t,
		})
	}

	return &GameStatus{
		Adding:   gsAdding,
		Schedule: schedule,
		Items:    gsItems,
		OnSale:   gsOnSale,
	}, nil
}

func serveGameConn(ws *websocket.Conn, roomName string) {
	log.Println(ws.RemoteAddr(), "serveGameConn", roomName)
	defer ws.Close()

	status, err := getStatus(roomName)
	if err != nil {
		printError(err)
		return
	}

	err = ws.WriteJSON(status)
	if err != nil {
		printError(err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chReq := make(chan GameRequest)

	go func() {
		defer cancel()
		for {
			req := GameRequest{}
			err := ws.ReadJSON(&req)
			if err != nil {
				printError(err)
				return
			}

			select {
			case chReq <- req:
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-chReq:
			log.Println(req)

			success := false
			switch req.Action {
			case "addIsu":
				success = addIsu(roomName, str2big(req.Isu), req.Time)
			case "buyItem":
				success = buyItem(roomName, req.ItemID, req.CountBought, req.Time)
			default:
				log.Println("Invalid Action")
				return
			}

			if success {
				// GameResponse を返却する前に 反映済みの GameStatus を返す
				status, err := getStatus(roomName)
				if err != nil {
					printError(err)
					return
				}

				err = ws.WriteJSON(status)
				if err != nil {
					printError(err)
					return
				}
			}

			err := ws.WriteJSON(GameResponse{
				RequestID: req.RequestID,
				IsSuccess: success,
			})
			if err != nil {
				printError(err)
				return
			}
		case <-ticker.C:
			status, err := getStatus(roomName)
			if err != nil {
				printError(err)
				return
			}

			err = ws.WriteJSON(status)
			if err != nil {
				printError(err)
				return
			}
		case <-ctx.Done():
			return
		}
	}
}
