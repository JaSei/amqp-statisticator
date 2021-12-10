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
	//conn, err := amqp.Dial(*rmqURI)
	cfg := new(tls.Config)
	if *rmqCaCert != "" {
		cacertfile, err := pathutil.New(*rmqCaCert)
		if err != nil {
			log.Fatalf("Open file %s fail: %s", *rmqCaCert, err)
		}

		cfg.RootCAs = x509.NewCertPool()

		ca, err := cacertfile.SlurpBytes()
		if err != nil {
			log.Fatalf("Read file %s fail: %s", *rmqCaCert, err)
		}
		cfg.RootCAs.AppendCertsFromPEM(ca)
	}
	conn, err := amqp.DialTLS(*rmqURI, cfg)
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

func (rmq AMQP) Close() error {
	rmq.channel.Cancel(consumerName, false)
	rmq.channel.Close()
	rmq.connection.Close()

	return nil
}
