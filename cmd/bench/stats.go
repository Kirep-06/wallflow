package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// BenchResult 记录单组压测的核心指标。
type BenchResult struct {
	Group       string
	Name        string
	Concurrency int
	TotalReqs   int
	SuccessReqs int
	ErrorReqs   int
	Duration    time.Duration
	QPS         float64
	AvgLatency  time.Duration
	P50Latency  time.Duration
	P95Latency  time.Duration
	P99Latency  time.Duration
	AvgBytes    int64
	TotalBytes  int64
	Extra       string
}

// ErrorRate 返回错误率百分比（0~100）。
func (r BenchResult) ErrorRate() float64 {
	if r.TotalReqs == 0 {
		return 0
	}
	return float64(r.ErrorReqs) * 100 / float64(r.TotalReqs)
}

// runLoad 以固定并发数执行 totalReqs 次任务并采集准确的分位延迟与吞吐。
func runLoad(
	ctx context.Context,
	group, name string,
	concurrency, totalReqs int,
	fn func(ctx context.Context, idx int) (bytesRead int64, err error),
) BenchResult {
	if concurrency <= 0 {
		concurrency = 1
	}
	if totalReqs <= 0 {
		return BenchResult{Group: group, Name: name, Concurrency: concurrency}
	}

	latencies := make([]time.Duration, totalReqs)
	var (
		nextIdx    atomic.Int64
		errCount   atomic.Int64
		totalBytes atomic.Int64
	)

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for range concurrency {
		go func() {
			defer wg.Done()
			for {
				i := int(nextIdx.Add(1) - 1)
				if i >= totalReqs {
					return
				}
				t0 := time.Now()
				n, err := fn(ctx, i)
				latencies[i] = time.Since(t0)
				if err != nil {
					errCount.Add(1)
				} else {
					totalBytes.Add(n)
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	var sum time.Duration
	for _, d := range latencies {
		sum += d
	}

	errs := int(errCount.Load())
	succ := totalReqs - errs
	avgBytes := int64(0)
	if succ > 0 {
		avgBytes = totalBytes.Load() / int64(succ)
	}

	qps := float64(totalReqs) / elapsed.Seconds()
	return BenchResult{
		Group:       group,
		Name:        name,
		Concurrency: concurrency,
		TotalReqs:   totalReqs,
		SuccessReqs: succ,
		ErrorReqs:   errs,
		Duration:    elapsed,
		QPS:         qps,
		AvgLatency:  sum / time.Duration(totalReqs),
		P50Latency:  percentile(latencies, 0.50),
		P95Latency:  percentile(latencies, 0.95),
		P99Latency:  percentile(latencies, 0.99),
		AvgBytes:    avgBytes,
		TotalBytes:  totalBytes.Load(),
	}
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// doHTTPRequest 执行 HTTP 请求并完整排空响应体（确保连接复用），同时校验预期状态码。
func doHTTPRequest(
	ctx context.Context,
	client *http.Client,
	method, url string,
	headers map[string]string,
	expectStatus int,
) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return 0, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	n, readErr := io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if readErr != nil {
		return n, readErr
	}
	if resp.StatusCode != expectStatus {
		return n, fmt.Errorf("unexpected status %d (want %d)", resp.StatusCode, expectStatus)
	}
	return n, nil
}

func formatDurationMS(d time.Duration) string {
	ms := float64(d.Microseconds()) / 1000.0
	if ms < 10 {
		return fmt.Sprintf("%.2f ms", ms)
	}
	return fmt.Sprintf("%.1f ms", ms)
}

func printResultTable(results []BenchResult) {
	fmt.Println()
	fmt.Println("| 分组 | 压测场景 | 并发 | 请求量 | QPS | P50 | P95 | P99 | 错误率 | 备注 |")
	fmt.Println("|---|---|---:|---:|---:|---:|---:|---:|---:|---|")
	for _, r := range results {
		fmt.Printf("| %s | %s | %d | %d | %.1f | %s | %s | %s | %.2f%% | %s |\n",
			r.Group,
			r.Name,
			r.Concurrency,
			r.TotalReqs,
			r.QPS,
			formatDurationMS(r.P50Latency),
			formatDurationMS(r.P95Latency),
			formatDurationMS(r.P99Latency),
			r.ErrorRate(),
			r.Extra,
		)
	}
	fmt.Println()
}
