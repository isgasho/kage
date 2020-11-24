package kage

import (
	"github.com/hamba/pkg/log"
	"github.com/msales/kage/kafka"
	"github.com/msales/kage/store"
)

// Store represents an offset store.
type Store interface {
	// SetState adds a state into the store.
	SetState(interface{}) error

	// BrokerOffsets returns a snapshot of the current broker offsets.
	BrokerOffsets() store.BrokerOffsets

	// ConsumerOffsets returns a snapshot of the current consumer group offsets.
	ConsumerOffsets() store.ConsumerOffsets

	// BrokerMetadata returns a snapshot of the current broker metadata.
	BrokerMetadata() store.BrokerMetadata

	// Channel get the offset channel.
	Channel() chan interface{}

	// Close gracefully stops the Store.
	Close()
}

// Monitor represents a Monitor monitor.
type Monitor interface {
	// Brokers returns a list of Kafka brokers.
	Brokers() []kafka.Broker

	// Collect collects the state of Monitor.
	Collect()

	// IsHealthy checks the health of the Monitor.
	IsHealthy() bool

	// Close gracefully stops the Monitor client.
	Close()
}

// Application represents the kage application.
type Application struct {
	Store     Store
	Reporters *Reporters
	Monitor   Monitor

	Logger log.Logger
}

// NewApplication creates an instance of Application.
func NewApplication() *Application {
	return &Application{}
}

// Collect collects the current state of the Kafka cluster.
func (a *Application) Collect() {
	a.Monitor.Collect()
}

// Report reports the current state of the MemoryStore to the Reporters.
func (a *Application) Report() {
	bo := a.Store.BrokerOffsets()
	a.Reporters.ReportBrokerOffsets(&bo)

	bm := a.Store.BrokerMetadata()
	a.Reporters.ReportBrokerMetadata(&bm)

	co := a.Store.ConsumerOffsets()
	a.Reporters.ReportConsumerOffsets(&co)
}

// IsHealthy checks the health of the Application.
func (a *Application) IsHealthy() bool {
	if a.Monitor == nil {
		return false
	}

	return a.Monitor.IsHealthy()
}

// Close gracefully shuts down the application.
func (a *Application) Close() {
	if a.Store != nil {
		a.Store.Close()
	}

	if a.Monitor != nil {
		a.Monitor.Close()
	}
}
