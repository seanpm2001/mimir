package blockbuilder

import (
	"context"
	"errors"
	"github.com/go-kit/log"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/util/testkafka"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"testing"
	"time"
)

func TestBlockBuilder_PartitionAssignment(t *testing.T) {
	// TODO: Implement this test
	// Test that when there are multiple partitons and block builders, the partitions are assigned properly
	// without an overlap in partitions and properly balanced. If this makes sense as an integration test,
	// it can be moved to the integration test file.
}

func TestBlockBuilder_BuildBlocks(t *testing.T) {
	// TODO: Implement this test
	// - Create test kafka cluster.
	// - add some samples to kafka topic/partitions before starting the blockbuilder.
	// - start a blockbuilder and test the correct production of blocks on startup.
	// - then add more samples and test the correct production of blocks after startup.
	// - Test both float and histogram samples.
	// - See test cases in tsdb_test.go for inspiration for various cases.
	// - also test the case of block builder consuming 1 vs more than 1 partition.

	const (
		topicName   = "test"
		partitionID = 0
		userID      = "1"
	)

	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(errors.New("test done")) })

	_, clusterAddr := testkafka.CreateCluster(t, 1, topicName)

	writeClient := newKafkaProduceClient(t, clusterAddr)
	for i := int64(0); i < 10; i++ {
		produceRecord(ctx, t, writeClient, topicName, partitionID, userID, createWriteRequest(t, floatSample(i), nil))
	}

	cfg := Config{
		Kafka: KafkaConfig{
			Address:       clusterAddr,
			Topic:         topicName,
			ClientID:      "1",
			DialTimeout:   10 * time.Second,
			ConsumerGroup: "testgroup",
		},
		BlocksStorageConfig: mimir_tsdb.BlocksStorageConfig{},
	}

	limits := defaultLimitsTestConfig()
	limits.OutOfOrderTimeWindow = 2 * model.Duration(time.Hour)
	limits.NativeHistogramsIngestionEnabled = true
	overrides, err := validation.NewOverrides(limits, nil)

	bb, err := NewBlockBuilder(cfg, log.NewNopLogger(), nil, overrides)
	require.NoError(t, err)
	_ = bb

	require.NoError(t, bb.StartAsync(ctx))
	require.NoError(t, bb.AwaitRunning(ctx))
	require.NoError(t, bb.AwaitTerminated(ctx))

	// This works where we dont mention group and do not disable auto commit
	//opts := []kgo.Opt{
	//	kgo.SeedBrokers(clusterAddr),
	//	kgo.ConsumeTopics(topicName),
	//}
	//
	//cl, err := kgo.NewClient(opts...)
	//require.NoError(t, err)
	//
	//for {
	//	fetches := cl.PollFetches(context.Background())
	//	if fetches.IsClientClosed() {
	//		return
	//	}
	//	fetches.EachError(func(_ string, _ int32, err error) {
	//		require.NoError(t, err)
	//	})
	//
	//	var seen int
	//	fetches.EachRecord(func(*kgo.Record) {
	//		seen++
	//	})
	//	fmt.Println("SEEN", seen)
	//	if seen > 0 {
	//		break
	//	}
	//}

}

func produceRecord(ctx context.Context, t *testing.T, writeClient *kgo.Client, topicName string, partitionID int32, userID string, content []byte) {
	rec := &kgo.Record{
		Key:       []byte(userID),
		Value:     content,
		Topic:     topicName,
		Partition: partitionID,
	}
	produceResult := writeClient.ProduceSync(ctx, rec)
	require.NoError(t, produceResult.FirstErr())
}

func newKafkaProduceClient(t *testing.T, addrs string) *kgo.Client {
	writeClient, err := kgo.NewClient(
		kgo.SeedBrokers(addrs),
		// We will choose the partition of each record.
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	require.NoError(t, err)
	t.Cleanup(writeClient.Close)
	return writeClient
}
