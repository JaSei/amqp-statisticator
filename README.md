# amqp-statisticator

Tool for collect statistics around your AMQP broker. For example RabbitMQ expose a lot information trought the management API, but size of messages missing. And when you want to calculate amount of data (for example for cloud native service calculation), you need it.

This tool have simple UI for fast lookup and it's possible to flush those data to file for deeper analysis.

## --help
```
usage: amqp-statisticator --uri=URI --exchange=EXCHANGE --queue=QUEUE [<flags>]

Flags:
  --help                   Show context-sensitive help (also try --help-long and --help-man).
  --uri=URI                RabbitMQ connection URI without query params - https://www.rabbitmq.com/uri-spec.html
  --amqpcacert=AMQPCACERT  custom CA cert file for rmq SSL connection
  --consumers=2            Amount of conusmers per queue
  --output=OUTPUT          json per line output file path
  --exchange=EXCHANGE ...  exchange what you want to consume
  --queue=QUEUE            destination queue for metricator
  --consumername="amqp-statisticator"
                           name of consumer registered in broker
```
