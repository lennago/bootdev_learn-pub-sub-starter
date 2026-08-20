package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func subscribe[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
	unmarshaller func([]byte) (T, error),
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("couldn't declare and bind queue: %v", err)
	}
	if err := ch.Qos(10, 0, false); err != nil {
		return fmt.Errorf("couldn't set QoS: %v", err)
	}
	msgs, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("couldn't consume messages: %v", err)
	}

	go func() {
		defer ch.Close()
		for msg := range msgs {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("couldn't unmarshal message: %v\n", err)
				continue
			}
			switch handler(target) {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			case NackDiscard:
				msg.Nack(false, false)
			default:
				fmt.Printf("unknown Acktype")
			}
		}
	}()
	return nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshaller,
	)
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange, queueName, key string,
	queueType SimpleQueueType,
	handler func(T) Acktype,
) error {
	unmarshaller := func(data []byte) (T, error) {
		var target T
		decoder := gob.NewDecoder(bytes.NewBuffer(data))
		err := decoder.Decode(&target)
		return target, err
	}
	return subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshaller,
	)
}
