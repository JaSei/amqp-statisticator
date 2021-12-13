package main

import (
	"crypto/tls"
	"crypto/x509"
	"log"

	"github.com/JaSei/pathutil-go"
	amqp "github.com/rabbitmq/amqp091-go"
)

type AMQP struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func newAmqp() AMQP {
	cfg := new(tls.Config)
	if *amqpCaCert != "" {
		cacertfile, err := pathutil.New(*amqpCaCert)
		if err != nil {
			log.Fatalf("Open file %s fail: %s", *amqpCaCert, err)
		}

		cfg.RootCAs = x509.NewCertPool()

		ca, err := cacertfile.SlurpBytes()
		if err != nil {
			log.Fatalf("Read file %s fail: %s", *amqpCaCert, err)
		}
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}
	conn, err := amqp.DialTLS(*amqpURI, cfg)
	if err != nil {
		log.Fatalf("connection.open: %s", err)
	}

	c, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel.open: %s", err)
	}

	if err = c.Qos(128, 0, false); err != nil {
		log.Fatalf("channel.qos: %s", err)
	}

	_, err = c.QueueDeclare(*queue, false, true, false, false, nil)
	if err != nil {
		log.Fatalf("queue.declare: %v", err)
	}

	for _, exchange := range *exchanges {

		err = c.QueueBind(*queue, "#", exchange, false, nil)
		if err != nil {
			log.Fatalf("queue.bind: %v", err)
		}
	}

	return AMQP{conn, c}
}

//StartConsumers start consumer pool defined by function
func (a AMQP) StartConsumers(queue string, countOfConsumers uint8, consumerFunc func(<-chan amqp.Delivery, chan<- ExchangeStats)) <-chan ExchangeStats {
	collector := make(chan ExchangeStats, 128)

	q, err := a.channel.Consume(queue, *consumerName, false, false, false, false, nil)
	for i := uint8(0); i < countOfConsumers; i++ {
		if err != nil {
			log.Fatalf("basic.consume: %v", err)
		}

		go consumerFunc(q, collector)
	}

	return collector
}

//Close close all channels and conncetions correctly
func (a AMQP) Close() {
	if err := a.channel.Cancel(*consumerName, false); err != nil {
		log.Printf("Cancel channel fail %s", err)
	}
	if err := a.channel.Close(); err != nil {
		log.Printf("Close channel fail %s", err)
	}
	if err := a.connection.Close(); err != nil {
		log.Printf("Close conncetion fail %s", err)
	}
}
