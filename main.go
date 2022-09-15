package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/JaSei/pathutil-go"
	"github.com/dustin/go-humanize"

	"code.rocketnine.space/tslocum/cview"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	amqpURI      = kingpin.Flag("uri", "RabbitMQ connection URI without query params - https://www.rabbitmq.com/uri-spec.html").Required().String()
	amqpCaCert   = kingpin.Flag("amqpcacert", "custom CA cert file for rmq SSL connection").String()
	consumers    = kingpin.Flag("consumers", "Amount of conusmers per queue").Default("2").Uint8()
	output       = kingpin.Flag("output", "json per line output file path").String()
	exchanges    = kingpin.Flag("exchange", "exchange what you want to consume").Required().Strings()
	queue        = kingpin.Flag("queue", "destination queue for metricator").Required().String()
	consumerName = kingpin.Flag("consumername", "name of consumer registered in broker").Default("amqp-statisticator").String()
)

var header = []string{"exchange", "routing key", "total messages", "total size", "messages per sec", "size per sec", "max size", "avg size"}

func main() {
	kingpin.Parse()

	ctx, cancel := context.WithCancel(context.Background())

	amqpConn := newAmqp(ctx)

	app := cview.NewApplication()
	defer app.HandlePanic()

	app.EnableMouse(true)

	table := cview.NewTable()
	table.SetBorders(true)

	for i, v := range header {
		table.SetCellSimple(0, i, v)
	}

	collector := amqpConn.StartConsumers(*queue, *consumers, consumer)

	amountOfStatsConsumers := uint8(1)
	if *output != "" {
		amountOfStatsConsumers++
	}

	finalStats := collectAggregationsAndMakeFinalStat(ctx, collector, amountOfStatsConsumers)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case statList := <-finalStats[0]:
				for i, stat := range statList {
					refreshTable(table, i, stat)
				}
				app.Draw()
			}
		}
	}()

	go func() {
		ticker := time.Tick(60 * time.Second)
		var lastStat []Stats

		if len(finalStats) < 2 {
			return
		}

		out, err := pathutil.New(*output)
		if err != nil {
			log.Fatal(err)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker:
				jOut, err := json.Marshal(lastStat)
				if err != nil {
					log.Fatal(err)
				}
				if err = out.Append(string(jOut) + "\n"); err != nil {
					log.Fatal(err)
				}

			case lastStat = <-finalStats[1]:
			}
		}
	}()

	app.SetRoot(table, true)
	if err := app.Run(); err != nil {
		panic(err)
	}

	log.Println("cancel")

	cancel()

	amqpConn.Close()
}

func consumer(ctx context.Context, q <-chan amqp.Delivery, collector chan<- ExchangeStats) {
	consumerAggregation := make(ExchangeStats)
	ticker := time.Tick(time.Second)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker:
			collector <- consumerAggregation
			consumerAggregation = make(ExchangeStats)
		case msg := <-q:
			exchange := ExchangeName(msg.Exchange)
			exchangeStat := consumerAggregation.get(exchange)

			routingKey := RoutingKeyName(msg.RoutingKey)
			routingStat := (*exchangeStat).get(routingKey)

			msgSize := uint64(len(msg.Body))
			routingStat.add(1, msgSize, msgSize)

			err := msg.Ack(false)
			if err != nil {
				log.Fatalf("Ack failed %s", err)
			}
		}
	}
}

func refreshTable(table *cview.Table, index int, stat Stats) {
	//index is shift because header
	index++
	count := new(big.Int).SetUint64(stat.Count)

	table.SetCellSimple(index, 0, string(stat.Exchange))
	table.SetCellSimple(index, 1, string(stat.RoutingKey))
	table.SetCellSimple(index, 2, humanize.BigComma(count))
	table.SetCellSimple(index, 3, humanize.Bytes(stat.TotalSize))
	table.SetCellSimple(index, 4, fmt.Sprintf("%0.1f", stat.AvgMsgPerSec))
	table.SetCellSimple(index, 5, fmt.Sprintf("%s/s", humanize.Bytes(stat.AvgBodySizePerSec)))
	table.SetCellSimple(index, 6, humanize.Bytes(stat.MaxSize))
	table.SetCellSimple(index, 7, humanize.Bytes(stat.AvgBodySize))
}

func collectAggregationsAndMakeFinalStat(ctx context.Context, collector <-chan ExchangeStats, amountOfStatsConsumers uint8) []chan []Stats {
	tick := time.Tick(time.Second)
	start := time.Now()
	total := make(ExchangeStats)

	statsChan := make([]chan []Stats, amountOfStatsConsumers)
	for i := uint8(0); i < amountOfStatsConsumers; i++ {
		statsChan[i] = make(chan []Stats, 2)
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case exchangeStat := <-collector:
				collectAggregates(&total, exchangeStat)
			case <-tick:
				dur := time.Since(start)

				statsList := make([]Stats, total.countOfRoutingKeys()+1)

				totalStat := RoutingStat{}

				id := 1
				for _, exchange := range total.sortedKeys() {
					exchangeStat := total[exchange]
					exchangeId := id
					id++

					wholeExchangeStat := RoutingStat{}
					exchangeKeys := exchangeStat.sortedKeys()
					for _, routingKey := range exchangeKeys {
						routingKeyStat := exchangeStat.get(routingKey)

						wholeExchangeStat.add((*routingKeyStat).Count, (*routingKeyStat).BodySize, (*routingKeyStat).MaxSize)
						totalStat.add((*routingKeyStat).Count, (*routingKeyStat).BodySize, (*routingKeyStat).MaxSize)

						routingStat := routingKeyStat.stats(exchange, routingKey, dur)
						statsList[id] = routingStat

						id++
					}

					totalStat := wholeExchangeStat.stats(exchange, RoutingKeyName(fmt.Sprintf("# (%d routing keys)", len(exchangeKeys))), dur)
					statsList[exchangeId] = totalStat
				}

				statsList[0] = totalStat.stats("_total_", "#", dur)

				for i := uint8(0); i < amountOfStatsConsumers; i++ {
					statsChan[i] <- statsList
				}
			}
		}
	}()

	return statsChan
}

func collectAggregates(total *ExchangeStats, lastStat ExchangeStats) {
	for ex, exStat := range lastStat {
		for routingKey, stat := range *exStat {
			total.get(ex).get(routingKey).add(stat.Count, stat.BodySize, stat.BodySize)
		}
	}
}
