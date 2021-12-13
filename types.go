package main

import (
	"sort"
	"time"
)

type RoutingStat struct {
	Count    uint64 `json:"count"`
	BodySize uint64 `json:"body_size"`
	MaxSize  uint64 `json:"max_size"`
}

func (st *RoutingStat) add(count, msgSize, maxSize uint64) {
	st.Count += count
	st.BodySize += msgSize

	if maxSize > st.MaxSize {
		st.MaxSize = maxSize
	}
}

type Stats struct {
	Exchange          ExchangeName   `json:"exchange"`
	RoutingKey        RoutingKeyName `json:"routing_key"`
	Count             uint64         `json:"count"`
	AvgMsgPerSec      float64        `json:"avg_msg_per_sec"`
	AvgBodySize       uint64         `json:"avg_body_size"`
	AvgBodySizePerSec uint64         `json:"avg_body_size_per_sec"`
	MaxSize           uint64         `json:"max_size"`
	TotalSize         uint64         `json:"total_size"`
}

func (st RoutingStat) stats(exchange ExchangeName, routingKey RoutingKeyName, dur time.Duration) Stats {
	ret := Stats{Exchange: exchange, RoutingKey: routingKey, Count: st.Count, MaxSize: st.MaxSize, TotalSize: st.BodySize}

	ret.AvgMsgPerSec = float64(st.Count) / dur.Seconds()
	ret.AvgBodySize = st.BodySize / st.Count
	ret.AvgBodySizePerSec = uint64(float64(st.BodySize) / dur.Seconds())

	return ret
}

type RoutingKeyName string

type MsgStats map[RoutingKeyName]*RoutingStat

func (msgStats MsgStats) sortedKeys() []RoutingKeyName {
	countOfKeys := len(msgStats)
	sortedKeys := make([]string, countOfKeys)
	i := 0
	for k := range msgStats {
		sortedKeys[i] = string(k)
		i++
	}
	sort.Strings(sortedKeys)

	ret := make([]RoutingKeyName, countOfKeys)
	for i, v := range sortedKeys {
		ret[i] = RoutingKeyName(v)
	}

	return ret
}

func (st MsgStats) get(key RoutingKeyName) *RoutingStat {
	ret, exist := st[key]
	if !exist {
		ret = &RoutingStat{}
		st[key] = ret
	}

	return ret
}

type ExchangeName string

type ExchangeStats map[ExchangeName]*MsgStats

func (ex ExchangeStats) countOfRoutingKeys() (ret uint) {
	for _, v := range ex {
		ret += uint(len(*v))
		ret++ //plus exchange # for total
	}

	return
}

func (ex ExchangeStats) sortedKeys() []ExchangeName {
	countOfKeys := len(ex)
	sortedKeys := make([]string, countOfKeys)
	i := 0
	for k := range ex {
		sortedKeys[i] = string(k)
		i++
	}
	sort.Strings(sortedKeys)

	ret := make([]ExchangeName, countOfKeys)
	for i, v := range sortedKeys {
		ret[i] = ExchangeName(v)
	}

	return ret
}

func (ex ExchangeStats) get(key ExchangeName) *MsgStats {
	ret, exist := ex[key]
	if !exist {
		a := make(MsgStats)
		ret = &a
		ex[key] = &a
	}

	return ret
}
