package ingester

import (
	"context"
	"flag"
	"fmt"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"
)

var flagvar int

const (
	millisecond         = 1
	concurrency_split   = 1000
	time_between_sample = 100 * millisecond
)

func init() {
	flag.IntVar(&flagvar, "numts", 10000, "number of timeseries")
}

func TestWriteNormalThroughPut(t *testing.T) {
	numT := flagvar
	scrapeBatch := 42300
	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)
	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester)
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})
	lbls := constructLabels(numT)

	tnow := time.Now()
	ingestNormal(t, lbls, ingester, ctx, numT, scrapeBatch)
	since := time.Since(tnow)

	throughput := 43200.0 * float64(numT) / float64(since.Seconds())
	t.Log(numT, since.Seconds(), throughput)
}

func ingestNormal(t *testing.T, lbls [][]mimirpb.LabelAdapter, ingester *Ingester, ctx context.Context, numTs int, scrapeBatch int) {
	var wg sync.WaitGroup
	const timeDelta = 100

	for i := 0; i < scrapeBatch; i += 100 {
		currTime := int64(i * timeDelta)
		lblsTemp := lbls
		for len(lblsTemp) > 0 {
			batch := lblsTemp[:concurrency_split]
			lblsTemp = lblsTemp[concurrency_split:]
			wg.Add(1)
			go func(currTime int64) {
				defer wg.Done()
				for j := 0; j < len(batch); j++ {
					samples := make([]mimirpb.Sample, 0, len(batch))
					ts := int64(j*timeDelta) + currTime
					for _, sample := range samples {
						sample.Value = rand.NormFloat64() * 100000
						sample.TimestampMs = ts
					}
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(batch, samples, nil, nil, mimirpb.API))
					require.NoError(t, err)
				}
			}(currTime)
		}
	}
	wg.Wait()
	fmt.Println("ingestion completed")
}

func TestWriteZipfThroughPut(t *testing.T) {
	numT := flagvar
	scrapeBatch := 42300
	registry := prometheus.NewRegistry()
	ctx := user.InjectOrgID(context.Background(), userID)
	// Create a mocked ingester
	cfg := defaultIngesterTestConfig(t)
	ingester, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), ingester))
	defer services.StopAndAwaitTerminated(context.Background(), ingester)
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return ingester.lifecycler.HealthyInstancesCount()
	})
	lbls := constructLabels(numT)

	tnow := time.Now()
	ingestZipf(t, lbls, ingester, ctx, numT, scrapeBatch)
	since := time.Since(tnow)

	throughput := 43200.0 * float64(numT) / float64(since.Seconds())
	t.Log(numT, since.Seconds(), throughput)
}

func ingestZipf(t *testing.T, lbls [][]mimirpb.LabelAdapter, ingester *Ingester, ctx context.Context, numTs int, scrapeBatch int) {
	var wg sync.WaitGroup
	const timeDelta = 100

	for i := 0; i < scrapeBatch; i += 100 {
		currTime := int64(i * timeDelta)
		lblsTemp := lbls
		for len(lblsTemp) > 0 {
			batch := lblsTemp[:concurrency_split]
			lblsTemp = lblsTemp[concurrency_split:]
			wg.Add(1)
			go func(currTime int64) {
				defer wg.Done()
				var s float64 = 1.01
				var v float64 = 1
				var RAND *rand.Rand = rand.New(rand.NewSource(time.Now().Unix()))
				z := rand.NewZipf(RAND, s, v, uint64(100000))
				for j := 0; j < len(batch); j++ {
					samples := make([]mimirpb.Sample, 0, len(batch))
					ts := int64(j*timeDelta) + currTime
					for _, sample := range samples {
						sample.Value = float64(z.Uint64())
						sample.TimestampMs = ts
					}
					_, err := ingester.Push(ctx, mimirpb.ToWriteRequest(batch, samples, nil, nil, mimirpb.API))
					require.NoError(t, err)
				}
			}(currTime)
		}
	}
	wg.Wait()
	fmt.Println("ingestion completed")

}

func constructLabels(numT int) (lbls [][]mimirpb.LabelAdapter) {
	for i := 0; i < numT; i++ {
		label := []mimirpb.LabelAdapter{{Name: labels.MetricName, Value: "fake_metric"},
			{Name: "machine", Value: strconv.Itoa(i)}}
		lbls = append(lbls, label)
	}

	return lbls
}
