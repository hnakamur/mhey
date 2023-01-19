package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"time"

	"github.com/urfave/cli/v2"
	"golang.org/x/exp/maps"
)

const ExitCodeUssage = 2

func main() {
	app := &cli.App{
		Name:  "mhey",
		Usage: "simple http load test tool for multiple sites",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "url",
				Usage:    "URLs (multiple values seperated by \",\")",
				Required: true,
			},
			&cli.StringSliceFlag{
				Name:     "host",
				Usage:    "hostss (multiple values seperated by \",\")",
				Required: true,
			},
			&cli.IntSliceFlag{
				Name:     "concurrency",
				Aliases:  []string{"c"},
				Usage:    "Concurrencies (multiple values seperated by \",\")",
				Required: true,
			},
			&cli.IntSliceFlag{
				Name:     "number",
				Aliases:  []string{"n"},
				Usage:    "number of requests (multiple values seperated by \",\")",
				Required: true,
			},
			&cli.Float64SliceFlag{
				Name:    "qps",
				Aliases: []string{"q"},
				Usage:   "request per second (default=0 is disabled)",
			},
			&cli.DurationFlag{
				Name:  "timeout",
				Usage: "request timeout",
				Value: 5 * time.Second,
			},
		},
		Action: func(cCtx *cli.Context) error {
			urls := cCtx.StringSlice("url")
			hosts := cCtx.StringSlice("host")
			concurrencies := cCtx.IntSlice("concurrency")
			numRequests := cCtx.IntSlice("number")
			qps := cCtx.Float64Slice("qps")
			reqTimeout := cCtx.Duration("timeout")

			if len(urls) == 0 {
				return cli.Exit("url flag must be set.", ExitCodeUssage)
			}
			if len(hosts) == 0 {
				return cli.Exit("host flag must be set.", ExitCodeUssage)
			}
			if len(concurrencies) == 0 {
				return cli.Exit("concurrency flag must be set.", ExitCodeUssage)
			}
			if len(numRequests) == 0 {
				return cli.Exit("number flag must be set", ExitCodeUssage)
			}

			if len(urls) != len(hosts) || len(urls) != len(concurrencies) || len(urls) != len(numRequests) || (len(qps) > 0 && len(urls) != len(qps)) {
				return cli.Exit("value count must be same among url, host, concurrency, and number flags.", ExitCodeUssage)
			}
			if !isValidConcurrencies(concurrencies) {
				return cli.Exit("concurrency value must be positive integer.", ExitCodeUssage)
			}
			if !isValidNumRuquests(numRequests, concurrencies) {
				return cli.Exit("number value must be multiple of concurrency.", ExitCodeUssage)
			}
			if !isValidQPS(qps) {
				return cli.Exit("QPS value must not be negative.", ExitCodeUssage)
			}

			return run(concurrencies, numRequests, urls, hosts, reqTimeout, qps)
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func isValidConcurrencies(concurrencies []int) bool {
	for _, c := range concurrencies {
		if c < 1 {
			return false
		}
	}
	return true
}

func isValidNumRuquests(numRequests, concurrencies []int) bool {
	for i, n := range numRequests {
		if n < 1 {
			return false
		}
		c := concurrencies[i]
		if n%c != 0 {
			return false
		}
	}
	return true
}

func isValidQPS(qps []float64) bool {
	for _, c := range qps {
		if c < 0 {
			return false
		}
	}
	return true
}

func run(concurrencies, numRequests []int, urls, hosts []string, reqTimeout time.Duration, qps []float64) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(qps) == 0 {
		qps = make([]float64, len(concurrencies))
	}

	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	log.Printf("start sending requests")

	client := &http.Client{Timeout: reqTimeout}
	var wg sync.WaitGroup
	wg.Add(len(concurrencies))
	errors := make([]error, len(concurrencies))
	statusCodesList := make([]map[int]int, len(concurrencies))
	elapsedList := make([]time.Duration, len(concurrencies))
	for i := range concurrencies {
		go func(i int) {
			defer wg.Done()

			t0 := time.Now()
			l := newLoader(client, urls[i], hosts[i], concurrencies[i], numRequests[i], qps[i])
			statusCodes, err := l.run(ctx)
			elapsedList[i] = time.Since(t0)
			if err != nil {
				errors[i] = err
				return
			}
			statusCodesList[i] = statusCodes
		}(i)
	}
	wg.Wait()
	log.Printf("finished sending requests")

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	for i, sc := range statusCodesList {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("site %d, url=%s, host=%s, concurrency=%d, numRequests=%d, qps=%g, elapsed=%s\n", i, urls[i], hosts[i], concurrencies[i], numRequests[i], qps[i], elapsedList[i])
		codes := maps.Keys(sc)
		sort.Ints(codes)
		for _, code := range codes {
			fmt.Printf("[%d]: %d requests\n", code, sc[code])
		}
	}

	return nil
}

type loader struct {
	client      *http.Client
	url         string
	host        string
	concurrency int
	numRequests int
	qps         float64
}

func newLoader(c *http.Client, url, host string, concurrency, numRequests int, qps float64) *loader {
	return &loader{
		client:      c,
		url:         url,
		host:        host,
		concurrency: concurrency,
		numRequests: numRequests,
		qps:         qps,
	}
}

func (l *loader) run(ctx context.Context) (statusCodes map[int]int, err error) {
	var wg sync.WaitGroup
	wg.Add(l.concurrency)
	errors := make([]error, l.concurrency)
	statusCodesList := make([]map[int]int, l.concurrency)
	for i := 0; i < l.concurrency; i++ {
		go func(i int) {
			defer wg.Done()

			r := newRequester(l.client, l.url, l.host, l.numRequests/l.concurrency, l.qps)
			if err := r.sendRequests(ctx); err != nil {
				errors[i] = err
				return
			}
			statusCodesList[i] = r.statusCodes
		}(i)
	}
	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return nil, err
		}
	}

	statusCodes = make(map[int]int)
	for _, sc := range statusCodesList {
		for code, count := range sc {
			if total, ok := statusCodes[code]; ok {
				statusCodes[code] = total + count
			} else {
				statusCodes[code] = count
			}
		}
	}
	return statusCodes, nil
}

type requester struct {
	client      *http.Client
	url         string
	host        string
	numRequests int
	qps         float64

	statusCodes map[int]int
}

func newRequester(c *http.Client, url, host string, numRequests int, qps float64) *requester {
	return &requester{
		client:      c,
		url:         url,
		host:        host,
		numRequests: numRequests,
		qps:         qps,
		statusCodes: make(map[int]int),
	}
}

func (r *requester) sendRequests(ctx context.Context) error {
	var throttle <-chan time.Time
	if r.qps > 0 {
		throttle = time.Tick(time.Duration(1e6/(r.qps)) * time.Microsecond)
	}
	for i := 0; i < r.numRequests; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			if r.qps > 0 {
				<-throttle
			}
			if err := r.sendRequest(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *requester) sendRequest() error {
	statusCode, err := r.doSendRequest()
	if err != nil {
		return err
	}

	if _, ok := r.statusCodes[statusCode]; ok {
		r.statusCodes[statusCode]++
	} else {
		r.statusCodes[statusCode] = 1
	}
	return nil
}

func (r *requester) doSendRequest() (statusCode int, err error) {
	req, err := http.NewRequest(http.MethodGet, r.url, nil)
	if err != nil {
		return 0, err
	}
	req.Host = r.host

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if _, err := io.Copy(io.Discard, resp.Body); err != nil {
		return 0, err
	}
	return resp.StatusCode, nil
}
