package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string
const (
	Durable SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var data []byte

	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	err = ch.PublishWithContext(
		context.Background(), 
		exchange, 
		key, 
		false, 
		false, 
		amqp.Publishing{
			ContentType: "application/json",
			Body: data,
		},
	)

	return nil
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	connChannel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := connChannel.QueueDeclare(
		queueName,                       // name
		queueType == SimpleQueueType(Durable), // durable
		queueType != SimpleQueueType(Durable), // delete when unused
		queueType != SimpleQueueType(Durable), // exclusive
		false,                           // no-wait                
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		}, // args
	)

	err = connChannel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return connChannel, queue, nil

}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	data := buf.Bytes()

	err = ch.PublishWithContext(
		context.Background(), 
		exchange, 
		key, 
		false, 
		false, 
		amqp.Publishing{
			ContentType: "application/gob",
			Body: data,
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func PublishGameLog(ch *amqp.Channel, username, message string) error {
	gamelog := routing.GameLog {
		CurrentTime: time.Now(),
		Message: message,
		Username: username,
	}

	routingKey := routing.GameLogSlug + "." + username

	err := PublishGob(
		ch,
		routing.ExchangePerilTopic,
		routingKey,
		gamelog,
	)

	if err != nil {
		return fmt.Errorf("Unable to publish game log: %w", err)
	}

	return nil
}