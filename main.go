package main

import (
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gdamore/tcell/v2"

	//ui "github.com/gizak/termui/v3"
	//"github.com/gizak/termui/v3/widgets"
	"code.rocketnine.space/tslocum/cview"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	rmqURI    = kingpin.Flag("rmq", "RabbitMQ connection URI without query params - https://www.rabbitmq.com/uri-spec.html").Required().String()
	rmqCaCert = kingpin.Flag("rmqcacert", "cacertfile for rmq SSL connection").String()
	consumers = kingpin.Flag("consumers", "Amount of conusmers per queue").Default("2").Uint8()
	output    = kingpin.Flag("output", "json per line output file path").String()
	exchanges = kingpin.Flag("exchange", "exchange what you want to consume").Required().Strings()
	queue     = kingpin.Flag("queue", "destination queue for metricator").Required().String()
)

const consumerName = "rabbit_statisticator"

var header = []string{"exchange", "routing key", "total messages", "messages per sec", "avg size", "size per sec", "max size", "total size"}

func main() {
	kingpin.Parse()

	amqp := newAmqp()

	app := cview.NewApplication()
	defer app.HandlePanic()

	app.EnableMouse(true)

	table := cview.NewTable()
	table.SetBorders(true)
	table.SetDoneFunc(func(key tcell.Key) {
		if key == tcell.KeyEscape {
			app.Stop()
		}
	})

	for i, v := range header {
		table.SetCellSimple(0, i, v)
	}

	//if err := ui.Init(); err != nil {
	//	log.Fatalf("failed to initialize termui: %v", err)
	//}
	//defer ui.Close()

	//termWidth, termHeight := ui.TerminalDimensions()

	//table := widgets.NewTable()
	//table.SetRect(0, 0, termWidth, termHeight)

	collector := make(chan ExchangeStats, 128)

	q, err := amqp.channel.Consume(*queue, consumerName, false, false, false, false, nil)
	for i := uint8(0); i < *consumers; i++ {
		if err != nil {
			log.Fatalf("basic.consume: %v", err)
		}

		go consumer(q, collector)
	}

	go func() {
		tick := time.Tick(time.Second)
		start := time.Now()
		total := make(ExchangeStats)
		for {
			select {
			case exchangeStat := <-collector:
				collectAggregates(&total, exchangeStat)
			case <-tick:
				dur := time.Since(start)

				//table.Rows = make([][]string, total.countOfRoutingKeys()+1)
				//table.Rows[0] = []string{"exchange", "routing key", "messages per sec", "avg messages", "avg size", "size per sec", "max size"}
				//table.RowStyles[0] = ui.NewStyle(ui.ColorWhite, ui.ColorBlack, ui.ModifierBold)

				id := 1
				for _, exchange := range total.sortedKeys() {
					exchangeStat := total[exchange]
					exchangeId := id
					id++

					total := RoutingStat{}
					for _, routingKey := range exchangeStat.sortedKeys() {
						routingStat := exchangeStat.get(routingKey)
						rs := routingStat.stats(dur)
						//editTable(table.Rows, id, "", routingKey, rs)
						//table.RowStyles[id] = ui.NewStyle(ui.ColorWhite)
						refreshTable(table, id, "", routingKey, rs)

						total.addWithMaxSize((*routingStat).Count, (*routingStat).BodySize, (*routingStat).MaxSize)

						id++
					}

					totalStat := total.stats(dur)
					//log.Print(totalStat, exchangeId)
					refreshTable(table, exchangeId, exchange, "#", totalStat)
					//table.RowStyles[exchangeId] = ui.NewStyle(ui.ColorWhite, ui.ColorBlue, ui.ModifierBold)

				}

				//ui.Render(table)
			}
		}
	}()

	//uiEvents := ui.PollEvents()
	//for {
	//	e := <-uiEvents
	//	switch e.ID {
	//	case "q", "<C-c>":
	//		return
	//	}
	//}

	go func() {
		ticker := time.Tick(time.Second)
		for {
			select {
			case <-ticker:
				app.Draw()
			}
		}
	}()

	app.SetRoot(table, true)
	if err := app.Run(); err != nil {
		panic(err)
	}
}

func consumer(q <-chan amqp.Delivery, collector chan<- ExchangeStats) {
	consumerAggregation := make(ExchangeStats)
	ticker := time.Tick(time.Second)

	for {
		select {
		case <-ticker:
			collector <- consumerAggregation
			consumerAggregation = make(ExchangeStats)
		case msg := <-q:
			exchange := ExchangeName(msg.Exchange)
			exchangeStat := consumerAggregation.get(exchange)

			routingKey := RoutingKeyName(msg.RoutingKey)
			routingStat := (*exchangeStat).get(routingKey)

			msgSize := uint64(len(msg.Body))
			routingStat.add(1, msgSize)

			msg.Ack(false)
		}
	}
}

//func editTable(row [][]string, index int, exchange ExchangeName, routingKey RoutingKeyName, stat Stats) {
//	count := new(big.Int).SetUint64(stat.count)
//	row[index] = []string{
//		string(exchange),
//		string(routingKey),
//		fmt.Sprintf("%s", humanize.BigComma(count)),
//		fmt.Sprintf("%0.1f", stat.avgMsgPerSec),
//		fmt.Sprintf("%s", humanize.Bytes(stat.avgBodySize)),
//		fmt.Sprintf("%s/s", humanize.Bytes(stat.avgBodySizePerSec)),
//		fmt.Sprintf("%s", humanize.Bytes(stat.maxSize)),
//	}
//}

func refreshTable(table *cview.Table, index int, exchange ExchangeName, routingKey RoutingKeyName, stat Stats) {
	count := new(big.Int).SetUint64(stat.count)

	table.SetCellSimple(index, 0, string(exchange))
	table.SetCellSimple(index, 1, string(routingKey))
	table.SetCellSimple(index, 2, humanize.BigComma(count))
	table.SetCellSimple(index, 3, fmt.Sprintf("%0.1f", stat.avgMsgPerSec))
	table.SetCellSimple(index, 4, humanize.Bytes(stat.avgBodySize))
	table.SetCellSimple(index, 5, fmt.Sprintf("%s/s", humanize.Bytes(stat.avgBodySizePerSec)))
	table.SetCellSimple(index, 6, humanize.Bytes(stat.maxSize))
	table.SetCellSimple(index, 7, humanize.Bytes(stat.totalSize))
}

func collectAggregates(total *ExchangeStats, lastStat ExchangeStats) {
	for ex, exStat := range lastStat {
		for routingKey, stat := range *exStat {
			total.get(ex).get(routingKey).add(stat.Count, stat.BodySize)
		}
	}
}
