package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hamba/pkg/log"
	"github.com/hamba/timex"
	"github.com/msales/kage/store"
	"github.com/ryanuber/go-glob"
)

// Broker represents a Kafka Broker.
type Broker struct {
	ID        int32
	Connected bool
}

// Monitor represents a Kafka cluster connection.
type Monitor struct {
	brokers []string

	client        sarama.Client
	refreshTicker *time.Ticker
	stateCh       chan interface{}

	ignoreTopics []string
	ignoreGroups []string

	log log.Logger
}

// New creates and returns a new Monitor for a Kafka cluster.
func New(opts ...MonitorFunc) (*Monitor, error) {
	monitor := &Monitor{}

	for _, o := range opts {
		o(monitor)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V0_10_1_0

	kafka, err := sarama.NewClient(monitor.brokers, config)
	if err != nil {
		return nil, err
	}
	monitor.client = kafka

	monitor.refreshTicker = time.NewTicker(2 * time.Minute)
	go func() {
		for range monitor.refreshTicker.C {
			monitor.refreshMetadata()
		}
	}()

	// Collect initial information
	go monitor.Collect()

	return monitor, nil
}

// Brokers returns a list of Kafka brokers.
func (m *Monitor) Brokers() []Broker {
	var brokers []Broker
	for _, b := range m.client.Brokers() {
		connected, _ := b.Connected()
		brokers = append(brokers, Broker{
			ID:        b.ID(),
			Connected: connected,
		})
	}
	return brokers
}

// Collect collects the state of Kafka.
func (m *Monitor) Collect() {
	m.getBrokerOffsets()
	m.getBrokerMetadata()
	m.getConsumerOffsets()
}

// IsHealthy checks the health of the Kafka cluster.
func (m *Monitor) IsHealthy() bool {
	for _, b := range m.client.Brokers() {
		if ok, _ := b.Connected(); ok {
			return true
		}
	}
	return false
}

// Close gracefully stops the Monitor.
func (m *Monitor) Close() {
	// Stop the offset ticker
	m.refreshTicker.Stop()
}

// getTopics gets the topics for the Kafka cluster.
func (m *Monitor) getTopics() map[string]int {
	// If auto create topics is on, trying to fetch metadata for a missing
	// topic will recreate it. To get around this we refresh the metadata
	// before getting topics and partitions.
	_ = m.client.RefreshMetadata()

	topicMap := make(map[string]int)

	topics, _ := m.client.Topics()
	for _, topic := range topics {
		partitions, _ := m.client.Partitions(topic)
		topicMap[topic] = len(partitions)
	}
	return topicMap
}

// refreshMetadata refreshes the broker metadata.
func (m *Monitor) refreshMetadata(topics ...string) {
	if err := m.client.RefreshMetadata(topics...); err != nil {
		m.log.Error("monitor: could not refresh topic metadata", "error", err)
	}
}

// getBrokerOffsets gets all broker topic offsets and sends them to the store.
func (m *Monitor) getBrokerOffsets() {
	topicMap := m.getTopics()

	reqs := make(map[int32]map[int64]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	for topic, partitions := range topicMap {
		if containsString(m.ignoreTopics, topic) {
			continue
		}

		for i := 0; i < partitions; i++ {
			broker, err := m.client.Leader(topic, int32(i))
			if err != nil {
				m.log.Error("monitor: topic leader error", "topic", topic, "partition", int32(i), "error", err)
				return
			}

			if _, ok := reqs[broker.ID()]; !ok {
				brokers[broker.ID()] = broker
				reqs[broker.ID()] = make(map[int64]*sarama.OffsetRequest)
				reqs[broker.ID()][sarama.OffsetOldest] = &sarama.OffsetRequest{}
				reqs[broker.ID()][sarama.OffsetNewest] = &sarama.OffsetRequest{}
			}

			reqs[broker.ID()][sarama.OffsetOldest].AddBlock(topic, int32(i), sarama.OffsetOldest, 1)
			reqs[broker.ID()][sarama.OffsetNewest].AddBlock(topic, int32(i), sarama.OffsetNewest, 1)
		}
	}

	var wg sync.WaitGroup
	getBrokerOffsets := func(brokerID int32, pos int64, req *sarama.OffsetRequest) {
		defer wg.Done()

		response, err := brokers[brokerID].GetAvailableOffsets(req)
		if err != nil {
			m.log.Error("monitor: cannot fetch offsets from broker", "broker", brokerID, "error", err)

			_ = brokers[brokerID].Close()

			return
		}

		ts := timex.Unix() * 1000
		for topic, partitions := range response.Blocks {
			for partition, offsetResp := range partitions {
				if offsetResp.Err != sarama.ErrNoError {
					if offsetResp.Err == sarama.ErrUnknownTopicOrPartition ||
						offsetResp.Err == sarama.ErrNotLeaderForPartition {
						// If we get this, the metadata is likely off, force a refresh for this topic
						m.refreshMetadata(topic)
						m.log.Info("monitor: metadata for topic %s refreshed due to OffsetResponse error", "topic", topic)
						continue
					}

					m.log.Error("monitor: error in OffsetResponse from broker",
						"topic", topic, "patition", partition, "broker", brokerID, "error", offsetResp.Err)
					continue
				}

				offset := &store.BrokerPartitionOffset{
					Topic:               topic,
					Partition:           partition,
					Oldest:              pos == sarama.OffsetOldest,
					Offset:              offsetResp.Offsets[0],
					Timestamp:           ts,
					TopicPartitionCount: topicMap[topic],
				}

				m.stateCh <- offset
			}
		}
	}

	for brokerID, requests := range reqs {
		for position, request := range requests {
			wg.Add(1)

			go getBrokerOffsets(brokerID, position, request)
		}
	}

	wg.Wait()
}

// getBrokerMetadata gets all broker topic metadata and sends them to the store.
func (m *Monitor) getBrokerMetadata() {
	var broker *sarama.Broker
	brokers := m.client.Brokers()
	for _, b := range brokers {
		if ok, _ := b.Connected(); ok {
			broker = b
			break
		}
	}

	if broker == nil {
		m.log.Error("monitor: no connected brokers found to collect metadata")
		return
	}

	resp, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		m.log.Error("monitor: cannot get metadata", "error", err)
		return
	}

	ts := time.Now().Unix() * 1000
	for _, topic := range resp.Topics {
		if containsString(m.ignoreTopics, topic.Name) {
			continue
		}
		if topic.Err != sarama.ErrNoError {
			m.log.Error("monitor: cannot get topic metadata", "topic", topic.Name, "error", topic.Err)
			continue
		}

		for _, partition := range topic.Partitions {
			if partition.Err != sarama.ErrNoError {
				m.log.Error("monitor: cannot get topic partition metadata",
					"topic", topic.Name, "partition", partition.ID, "error", partition.Err)
				continue
			}

			m.stateCh <- &store.BrokerPartitionMetadata{
				Topic:               topic.Name,
				Partition:           partition.ID,
				TopicPartitionCount: len(topic.Partitions),
				Leader:              partition.Leader,
				Replicas:            partition.Replicas,
				Isr:                 partition.Isr,
				Timestamp:           ts,
			}
		}
	}
}

// getConsumerOffsets gets all the consumer offsets and send them to the store.
func (m *Monitor) getConsumerOffsets() {
	topicMap := m.getTopics()
	reqs := make(map[int32]map[string]*sarama.OffsetFetchRequest)
	coords := make(map[int32]*sarama.Broker)

	brokers := m.client.Brokers()
	for _, broker := range brokers {
		if ok, err := broker.Connected(); !ok {
			if err != nil {
				m.log.Error("monitor: failed to connect to broker broker", "broker", broker.ID(), "error", err)
				continue
			}

			if err := broker.Open(m.client.Config()); err != nil {
				m.log.Error("monitor: failed to connect to broker broker", "broker", broker.ID(), "error", err)
				continue
			}
		}

		groups, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			m.log.Error("monitor: cannot fetch consumer groups on broker", "broker", broker.ID(), "error", err)
			continue
		}

		for group := range groups.Groups {
			if containsString(m.ignoreGroups, group) {
				continue
			}

			coord, err := m.client.Coordinator(group)
			if err != nil {
				m.log.Error("monitor: cannot fetch co-ordinator for group", "group", group, "error", err)
				continue
			}

			if _, ok := reqs[coord.ID()]; !ok {
				coords[coord.ID()] = coord
				reqs[coord.ID()] = make(map[string]*sarama.OffsetFetchRequest)
			}

			if _, ok := reqs[coord.ID()][group]; !ok {
				reqs[coord.ID()][group] = &sarama.OffsetFetchRequest{ConsumerGroup: group, Version: 1}
			}

			for topic, partitions := range topicMap {
				for i := 0; i < partitions; i++ {
					reqs[coord.ID()][group].AddPartition(topic, int32(i))
				}
			}
		}
	}

	var wg sync.WaitGroup
	getConsumerOffsets := func(brokerID int32, group string, req *sarama.OffsetFetchRequest) {
		defer wg.Done()

		offsets, err := coords[brokerID].FetchOffset(req)
		if err != nil {
			m.log.Error("monitor: cannot get group topic offsets", "broker", brokerID, "error", err)

			return
		}

		ts := timex.Unix() * 1000
		for topic, partitions := range offsets.Blocks {
			for partition, block := range partitions {
				if block.Err != sarama.ErrNoError {
					m.log.Error("monitor: cannot get group topic offsets", "broker", brokerID, "error", block.Err)
					continue
				}

				if block.Offset == -1 {
					// We don't have an offset for this topic partition, ignore.
					continue
				}

				offset := &store.ConsumerPartitionOffset{
					Group:     group,
					Topic:     topic,
					Partition: partition,
					Offset:    block.Offset,
					Timestamp: ts,
				}

				m.stateCh <- offset
			}
		}
	}

	for brokerID, groups := range reqs {
		for group, request := range groups {
			wg.Add(1)

			go getConsumerOffsets(brokerID, group, request)
		}
	}

	wg.Wait()
}

// containsString determines if the string matches any of the provided patterns.
func containsString(patterns []string, subject string) bool {
	for _, pattern := range patterns {
		if glob.Glob(pattern, subject) {
			return true
		}
	}

	return false
}
