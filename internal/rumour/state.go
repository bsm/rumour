package rumour

import (
	"sort"
	"sync"
)

// State maintains all state
type State struct {
	clusters map[string]*ClusterState
}

// NewState inits a state.
func NewState(clusters []string) *State {
	if len(clusters) == 0 {
		clusters = []string{"default"}
	}
	sub := make(map[string]*ClusterState, len(clusters))
	for _, name := range clusters {
		sub[name] = NewClusterState()
	}
	return &State{clusters: sub}
}

// Clusters returns the cluster names.
func (s *State) Clusters() []string {
	names := make([]string, 0, len(s.clusters))
	for name := range s.clusters {
		names = append(names, name)
	}

	sort.Strings(names)
	return names
}

// Cluster returns state by name.
func (s *State) Cluster(name string) *ClusterState {
	return s.clusters[name]
}

// --------------------------------------------------------------------

// ConsumerTopic maintains group topic info.
type ConsumerTopic struct {
	Topic     string           `json:"topic"`
	Timestamp int64            `json:"timestamp"`
	Offsets   []ConsumerOffset `json:"offsets"`
}

type consumerTopics []ConsumerTopic

func (p consumerTopics) Len() int           { return len(p) }
func (p consumerTopics) Less(i, j int) bool { return p[i].Topic < p[j].Topic }
func (p consumerTopics) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// ConsumerOffset maintains partition offsets for a consumer.
type ConsumerOffset struct {
	Offset int64 `json:"offset"`
	Lag    int64 `json:"lag"`
}

func calcConsumerOffsets(maxima, offsets []int64) []ConsumerOffset {
	res := make([]ConsumerOffset, len(maxima))
	for i, max := range maxima {
		var off int64
		if i < len(offsets) {
			off = offsets[i]
			res[i].Offset = off
		}
		if off < max {
			res[i].Lag = max - off
		}
	}
	return res
}

// --------------------------------------------------------------------

type consumerOffsetState struct {
	Offsets   []int64
	Timestamp int64
}

// ClusterState maintains cluster state.
type ClusterState struct {
	brokers   []string
	topics    map[string][]int64
	consumers map[string]map[string]consumerOffsetState
	mu        sync.RWMutex
}

// NewClusterState inits a cluster state.
func NewClusterState() *ClusterState {
	return &ClusterState{
		topics:    make(map[string][]int64),
		consumers: make(map[string]map[string]consumerOffsetState),
	}
}

// Brokers returns the broker addresses.
func (s *ClusterState) Brokers() []string {
	s.mu.RLock()
	addrs := make([]string, len(s.brokers))
	copy(addrs, s.brokers)
	s.mu.RUnlock()

	sort.Strings(addrs)
	return addrs
}

// UpdateBrokers updates brokers addresses.
func (s *ClusterState) UpdateBrokers(brokers []string) {
	s.mu.Lock()
	s.brokers = brokers
	s.mu.Unlock()
}

// Topics returns the topic names.
func (s *ClusterState) Topics() []string {
	s.mu.RLock()
	names := make([]string, 0, len(s.topics))
	for name := range s.topics {
		names = append(names, name)
	}
	s.mu.RUnlock()

	sort.Strings(names)
	return names
}

// TopicOffsets returns offsets for a topic.
func (s *ClusterState) TopicOffsets(topic string) ([]int64, bool) {
	s.mu.RLock()
	offsets, ok := s.topics[topic]
	s.mu.RUnlock()

	return offsets, ok
}

// UpdateTopic updates topic offsets.
func (s *ClusterState) UpdateTopic(name string, offsets []int64) {
	s.mu.Lock()
	s.topics[name] = offsets
	s.mu.Unlock()
}

// ConsumerGroups returns consumer group names.
func (s *ClusterState) ConsumerGroups() []string {
	s.mu.RLock()
	groups := make([]string, 0, len(s.topics))
	for group := range s.consumers {
		groups = append(groups, group)
	}
	s.mu.RUnlock()

	sort.Strings(groups)
	return groups
}

// ConsumerTopics returns a summary of ConsumerTopics.
func (s *ClusterState) ConsumerTopics(group string) ([]ConsumerTopic, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if topics, ok := s.consumers[group]; ok {
		res := make([]ConsumerTopic, 0, len(topics))
		for topic, cos := range topics {
			if maxima, ok := s.topics[topic]; ok {
				res = append(res, ConsumerTopic{
					Topic:     topic,
					Timestamp: cos.Timestamp,
					Offsets:   calcConsumerOffsets(maxima, cos.Offsets),
				})
			}
		}
		sort.Sort(consumerTopics(res))
		return res, true
	}
	return nil, false
}

// UpdateConsumerOffsets updates consumer offsets.
func (s *ClusterState) UpdateConsumerOffsets(group, topic string, timestamp int64, offsets []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topics, ok := s.consumers[group]
	if !ok {
		topics = make(map[string]consumerOffsetState)
	}
	if timestamp >= topics[topic].Timestamp {
		topics[topic] = consumerOffsetState{Offsets: offsets, Timestamp: timestamp}
	}
	s.consumers[group] = topics
}
