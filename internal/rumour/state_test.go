package rumour_test

import (
	"github.com/bsm/rumour/internal/rumour"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("State", func() {
	var subject *rumour.State

	BeforeEach(func() {
		subject = rumour.NewState([]string{"default", "other"})
	})

	It("should returns clusters", func() {
		Expect(rumour.NewState(nil).Clusters()).To(Equal([]string{"default"}))
		Expect(subject.Clusters()).To(Equal([]string{"default", "other"}))
	})

	It("should find clusters", func() {
		Expect(subject.Cluster("default")).To(BeAssignableToTypeOf(&rumour.ClusterState{}))
		Expect(subject.Cluster("missing")).To(BeNil())
	})
})

var _ = Describe("ClusterState", func() {
	var subject *rumour.ClusterState

	BeforeEach(func() {
		subject = rumour.NewClusterState()

		subject.UpdateBrokers([]string{"10.0.0.2:9092", "10.0.0.1:9092"})

		subject.UpdateTopic("two-topic", []int64{117, 125, 101, 124})
		subject.UpdateTopic("one-topic", []int64{125, 101, 117, 124})

		subject.UpdateConsumerOffsets("csmy", "two-topic", []int64{125, 100, 117, 124})
		subject.UpdateConsumerOffsets("csmx", "one-topic", []int64{120, 101, 117, 115})
		subject.UpdateConsumerOffsets("csmx", "two-topic", []int64{999, 125, 100})
	})

	It("should read brokers", func() {
		Expect(subject.Brokers()).To(Equal([]string{"10.0.0.1:9092", "10.0.0.2:9092"}))
	})

	It("should read topics", func() {
		Expect(subject.Topics()).To(Equal([]string{"one-topic", "two-topic"}))

		offsets, ok := subject.TopicOffsets("one-topic")
		Expect(ok).To(BeTrue())
		Expect(offsets).To(Equal([]int64{125, 101, 117, 124}))

		_, ok = subject.TopicOffsets("missing")
		Expect(ok).To(BeFalse())
	})

	It("should read consumer groups", func() {
		Expect(subject.ConsumerGroups()).To(Equal([]string{"csmx", "csmy"}))

		topics, ok := subject.ConsumerTopics("csmx")
		Expect(ok).To(BeTrue())
		Expect(topics).To(Equal([]rumour.ConsumerTopic{
			{Topic: "one-topic", Offsets: []rumour.ConsumerOffset{
				{Offset: 120, Lag: 5},
				{Offset: 101, Lag: 0},
				{Offset: 117, Lag: 0},
				{Offset: 115, Lag: 9},
			}},
			{Topic: "two-topic", Offsets: []rumour.ConsumerOffset{
				{Offset: 999, Lag: 0},
				{Offset: 125, Lag: 0},
				{Offset: 100, Lag: 1},
				{Offset: 0, Lag: 124},
			}},
		}))

		_, ok = subject.ConsumerTopics("missing")
		Expect(ok).To(BeFalse())
	})
})
