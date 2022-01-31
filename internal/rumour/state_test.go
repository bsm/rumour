package rumour_test

import (
	"github.com/bsm/rumour/internal/rumour"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
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

		subject.UpdateConsumerOffsets("csmy", "two-topic", 1515151515, []int64{125, 100, 117, 124})
		subject.UpdateConsumerOffsets("csmx", "one-topic", 1515151516, []int64{120, 101, 117, 115})
		subject.UpdateConsumerOffsets("csmx", "two-topic", 1515151517, []int64{999, 125, 100})
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
			{
				Topic:     "one-topic",
				Timestamp: 1515151516,
				Offsets: []rumour.ConsumerOffset{
					{Offset: 120, Lag: 5},
					{Offset: 101, Lag: 0},
					{Offset: 117, Lag: 0},
					{Offset: 115, Lag: 9},
				},
			},
			{
				Topic:     "two-topic",
				Timestamp: 1515151517,
				Offsets: []rumour.ConsumerOffset{
					{Offset: 999, Lag: 0},
					{Offset: 125, Lag: 0},
					{Offset: 100, Lag: 1},
					{Offset: 0, Lag: 124},
				},
			},
		}))

		_, ok = subject.ConsumerTopics("missing")
		Expect(ok).To(BeFalse())
	})

	It("should expire consumer groups", func() {
		subject.ExpireConsumerGroups(1515151500)
		Expect(subject.ConsumerGroups()).To(Equal([]string{"csmx", "csmy"}))
		topix, _ := subject.ConsumerTopics("csmx")
		Expect(topix).To(HaveLen(2))
		topiy, _ := subject.ConsumerTopics("csmy")
		Expect(topiy).To(HaveLen(1))

		subject.ExpireConsumerGroups(1515151517)
		Expect(subject.ConsumerGroups()).To(Equal([]string{"csmx"}))
		topix, _ = subject.ConsumerTopics("csmx")
		Expect(topix).To(HaveLen(1))

		subject.ExpireConsumerGroups(1515151520)
		Expect(subject.ConsumerGroups()).To(BeEmpty())
	})
})
