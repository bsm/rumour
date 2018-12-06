package rumour

import (
	"context"
	"errors"
	"log"
	"os"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

// ClusterConfig contains cluster config info.
type ClusterConfig struct {
	Name          string        `ignored:"true"`
	Brokers       []string      `required:"true"`
	MetaRefresh   time.Duration `default:"180s"`
	OffsetRefresh time.Duration `default:"30s"`
}

// Fetcher updates state.
type Fetcher struct {
	clusters []ClusterConfig
	logger   *log.Logger

	mu sync.Mutex
}

// NewFetcher inits a fetcher.
func NewFetcher(clusters ...ClusterConfig) (*Fetcher, error) {
	if len(clusters) == 0 {
		return nil, errors.New("rumour: list of monitored clusters cannot be empty")
	}

	return &Fetcher{
		logger:   log.New(os.Stdout, "[fetch] ", log.LstdFlags),
		clusters: clusters,
	}, nil
}

// RunLoop starts the blocking loop.
func (f *Fetcher) RunLoop(ctx context.Context, state *State) {
	f.mu.Lock()
	defer f.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	names := make([]string, 0, len(f.clusters))
	for _, cc := range f.clusters {
		names = append(names, cc.Name)
	}

	wg := new(sync.WaitGroup)
	for _, cc := range f.clusters {
		wg.Add(1)
		go func(c *ClusterConfig) {
			defer wg.Done()

			for {
				if isDone(ctx) {
					return
				}
				f.monitor(ctx, c, state.Cluster(c.Name))
			}
		}(&cc)
	}
	wg.Wait()
}

func (f *Fetcher) monitor(ctx context.Context, cc *ClusterConfig, state *ClusterState) {
	config := sarama.NewConfig()
	config.ClientID = "rumour"
	config.Version = sarama.V0_10_0_0

	client, err := sarama.NewClient(cc.Brokers, config)
	if err != nil {
		f.logger.Printf("error connecting to %q: %v", cc.Name, err)
		return
	}
	defer client.Close()

	cf := &clusterFetcher{
		client: client,
		state:  state,
		groups: make(map[string][]string),
	}

	mtt := time.NewTimer(0)
	defer mtt.Stop()

	ott := time.NewTimer(0)
	defer ott.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-mtt.C:
			start := time.Now()
			if cf.refreshMeta(ctx); err != nil {
				f.logger.Printf("error refreshing groups for %q: %v", cc.Name, err)
				mtt.Reset(30 * time.Second) // try again in 30s
			} else {
				f.logger.Printf("refreshed metadata for %q in %.3fs", cc.Name, time.Since(start).Seconds())
				mtt.Reset(cc.MetaRefresh)
			}
		case <-ott.C:
			start := time.Now()
			if cf.refreshOffsets(ctx); err != nil {
				f.logger.Printf("error refreshing offsets for %q: %v", cc.Name, err)
				ott.Reset(30 * time.Second) // try again in 30s
			} else {
				f.logger.Printf("refreshed offsets for %q in %.3fs", cc.Name, time.Since(start).Seconds())
				ott.Reset(cc.OffsetRefresh)
			}
		}
	}
}

type clusterFetcher struct {
	client sarama.Client
	state  *ClusterState
	groups map[string][]string // group -> topics mapping
}

func (f *clusterFetcher) refreshMeta(ctx context.Context) error {
	// populate brokers
	brokers := f.client.Brokers()
	addrs := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		addrs = append(addrs, broker.Addr())
	}
	f.state.UpdateBrokers(addrs)

	// query brokers for known groups
	for _, broker := range brokers {
		_ = broker.Open(f.client.Config())

		lres, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return err
		} else if lres.Err != sarama.ErrNoError {
			return lres.Err
		}

		// prepare describe consumer groups request
		dreq := new(sarama.DescribeGroupsRequest)
		for group, kind := range lres.Groups {
			if kind == "consumer" {
				dreq.AddGroup(group)
			}
		}
		if len(dreq.Groups) == 0 {
			continue
		}

		// fetch descriptions
		dres, err := broker.DescribeGroups(dreq)
		if err != nil {
			return err
		}

		if err := f.extractGroupTopicAssociations(dres); err != nil {
			return err
		}
	}
	return nil
}

func (f *clusterFetcher) refreshOffsets(ctx context.Context) error {
	if err := f.refreshTopics(ctx); err != nil {
		return err
	}

	// refresh groups
	for group, topics := range f.groups {
		if isDone(ctx) {
			return nil
		}
		if err := f.refreshGroupOffsets(ctx, group, topics); err != nil {
			return err
		}
	}
	return nil
}

func (f *clusterFetcher) refreshTopics(ctx context.Context) error {
	topics, err := f.client.Topics()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		if isDone(ctx) {
			return nil
		}

		partitions, err := f.client.Partitions(topic)
		if err != nil {
			return err
		}

		size := 0
		for _, part := range partitions {
			if n := int(part) + 1; n > size {
				size = n
			}
		}

		offsets := make([]int64, size)
		for _, part := range partitions {
			off, err := f.client.GetOffset(topic, part, sarama.OffsetNewest)
			if err != nil {
				return err
			}
			offsets[int(part)] = off
		}
		f.state.UpdateTopic(topic, offsets)
	}
	return nil
}

func (f *clusterFetcher) refreshGroupOffsets(ctx context.Context, group string, topics []string) error {
	req := new(sarama.OffsetFetchRequest)
	req.Version = 1
	req.ConsumerGroup = group
	for _, topic := range topics {
		if isDone(ctx) {
			return nil
		}
		partitions, err := f.client.Partitions(topic)
		if err != nil {
			return err
		}
		for _, part := range partitions {
			req.AddPartition(topic, part)
		}
	}

	// refresh coordinator
	if err := f.client.RefreshCoordinator(group); err != nil {
		return err
	}

	// find coordinator
	broker, err := f.client.Coordinator(group)
	if err != nil {
		return err
	}

	// fetch the offsets
	resp, err := broker.FetchOffset(req)
	if err != nil {
		return err
	}

	for topic, blocks := range resp.Blocks {
		size := 0
		for part, block := range blocks {
			if block.Err != sarama.ErrNoError {
				return block.Err
			}
			if n := int(part) + 1; n > size {
				size = n
			}
		}

		offsets := make([]int64, size)
		for part, block := range blocks {
			offsets[int(part)] = block.Offset
		}
		f.state.UpdateConsumerOffsets(group, topic, time.Now().Unix(), offsets)
	}
	return nil
}

func (f *clusterFetcher) extractGroupTopicAssociations(res *sarama.DescribeGroupsResponse) error {
	for _, group := range res.Groups {
		if group.Err != sarama.ErrNoError {
			return group.Err
		}

		topics := make(map[string]struct{})
		for _, mem := range group.Members {
			meta, err := mem.GetMemberMetadata()
			if err != nil {
				return err
			}
			for _, topic := range meta.Topics {
				topics[topic] = struct{}{}
			}
		}

		f.groups[group.GroupId] = f.groups[group.GroupId][:0]
		for topic := range topics {
			f.groups[group.GroupId] = append(f.groups[group.GroupId], topic)
		}
	}
	return nil
}

func isDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
