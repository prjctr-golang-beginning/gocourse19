package brief

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

type Master struct {
	qName           string
	connection      *amqp.Connection
	done            chan struct{}
	notifyConnClose chan *amqp.Error
	isReady         bool
}

func NewMaster(ctx context.Context, qName, busHost, busUser, busPass string) (*Master, error) {
	session := Master{
		qName: qName,
		done:  make(chan struct{}),
	}

	err := session.connect(ctx, busHost, busUser, busPass)
	if err != nil {
		return nil, err
	}

	return &session, nil
}

func (s *Master) connect(_ context.Context, busHost, busUser, busPass string) error {
	conn, err := amqp.DialConfig(busHost, amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{busUser, busPass}},
	})

	if err != nil {
		return err
	}

	s.connection = conn
	s.notifyConnClose = make(chan *amqp.Error, 1)
	s.connection.NotifyClose(s.notifyConnClose)
	s.isReady = true
	log.Println("Consumer: CONNECTED")

	return nil
}

func (s *Master) Close() error {
	if !s.isReady {
		return errors.New(fmt.Sprintf("Consumer: connection not ready while closing"))
	}
	err := s.connection.Close()
	if err != nil {
		return err
	}
	s.isReady = false

	return nil
}

func (s *Master) Complete() {
	s.done <- struct{}{}
}
