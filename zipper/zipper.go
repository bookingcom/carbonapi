package zipper

import (
	"context"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-graphite/carbonapi/cfg"
	"github.com/go-graphite/carbonapi/limiter"
	"github.com/go-graphite/carbonapi/pathcache"
	"github.com/go-graphite/carbonapi/util"
	pb3 "github.com/go-graphite/protocol/carbonapi_v2_pb"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

// Zipper provides interface to Zipper-related functions
type Zipper struct {
	storageClient *http.Client
	// Limiter limits our concurrency to a particular server
	limiter     limiter.ServerLimiter
	probeTicker *time.Ticker
	ProbeQuit   chan struct{}
	ProbeForce  chan int

	timeoutAfterAllStarted time.Duration
	timeout                time.Duration
	timeoutConnect         time.Duration
	keepAliveInterval      time.Duration

	searchBackend    string
	searchConfigured bool
	searchPrefix     string

	pathCache   pathcache.PathCache
	searchCache pathcache.PathCache

	backends                  []string
	concurrencyLimitPerServer int
	maxIdleConnsPerHost       int

	sendStats func(*Stats)

	logger *zap.Logger
}

func (z Zipper) LimiterUse() map[string]float64 {
	return z.limiter.LimiterUse()
}

func (z Zipper) MaxLimiterUse() float64 {
	return z.limiter.MaxLimiterUse()
}

// Stats provides zipper-related statistics
type Stats struct {
	Timeouts          int64
	FindErrors        int64
	RenderErrors      int64
	InfoErrors        int64
	SearchRequests    int64
	SearchCacheHits   int64
	SearchCacheMisses int64

	MemoryUsage int64

	CacheMisses int64
	CacheHits   int64
}

type nameLeaf struct {
	name string
	leaf bool
}

// NewZipper allows to create new Zipper
func NewZipper(sender func(*Stats), config cfg.Zipper, logger *zap.Logger) *Zipper {
	z := &Zipper{
		probeTicker: time.NewTicker(10 * time.Minute),
		ProbeQuit:   make(chan struct{}),
		ProbeForce:  make(chan int),

		sendStats: sender,

		pathCache:   config.PathCache,
		searchCache: config.SearchCache,

		storageClient:             &http.Client{},
		backends:                  config.Common.Backends,
		searchBackend:             config.CarbonSearch.Backend,
		searchPrefix:              config.CarbonSearch.Prefix,
		searchConfigured:          len(config.CarbonSearch.Prefix) > 0 && len(config.CarbonSearch.Backend) > 0,
		concurrencyLimitPerServer: config.ConcurrencyLimitPerServer,
		maxIdleConnsPerHost:       config.MaxIdleConnsPerHost,
		keepAliveInterval:         config.KeepAliveInterval,
		timeoutAfterAllStarted:    config.Timeouts.AfterStarted,
		timeout:                   config.Timeouts.Global,
		timeoutConnect:            config.Timeouts.Connect,

		logger: logger,
	}

	logger.Info("zipper config",
		zap.Any("config", config),
	)

	if z.concurrencyLimitPerServer != 0 {
		limiterServers := z.backends
		if z.searchConfigured {
			limiterServers = append(limiterServers, z.searchBackend)
		}
		z.limiter = limiter.NewServerLimiter(limiterServers, z.concurrencyLimitPerServer)
	}

	// configure the storage client
	z.storageClient.Transport = &http.Transport{
		MaxIdleConnsPerHost: z.maxIdleConnsPerHost,
		DialContext: (&net.Dialer{
			Timeout:   z.timeoutConnect,
			KeepAlive: z.keepAliveInterval,
			DualStack: true,
		}).DialContext,
	}

	go z.probeTlds()

	z.ProbeForce <- 1
	return z
}

// ServerResponse contains response from the zipper
type ServerResponse struct {
	server   string
	response []byte
	err      error
}

var (
	errNoResponses      = "No responses fetched from upstream"
	errNoMetricsFetched = "No metrics in the response"
	errBadResponseCode  = "Bad response code"
)

type byStepTime []pb3.FetchResponse

func (s byStepTime) Len() int { return len(s) }

func (s byStepTime) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s byStepTime) Less(i, j int) bool {
	return s[i].GetStepTime() < s[j].GetStepTime()
}

func (z *Zipper) mergeResponses(responses []ServerResponse, stats *Stats) ([]string, *pb3.MultiFetchResponse) {
	logger := z.logger.With(zap.String("function", "mergeResponses"))

	servers := make([]string, 0, len(responses))
	metrics := make(map[string][]pb3.FetchResponse)

	for _, r := range responses {
		var d pb3.MultiFetchResponse
		err := d.Unmarshal(r.response)
		if err != nil {
			err = errors.WithStack(err)
			logger.Error("error decoding protobuf response",
				zap.String("server", r.server),
				zap.String("error", fmt.Sprintf("%+v", err)),
			)

			if ce := logger.Check(zap.DebugLevel, "response hexdump"); ce != nil {
				ce.Write(
					zap.String("response", hex.Dump(r.response)),
				)
			}
			stats.RenderErrors++
			continue
		}
		stats.MemoryUsage += int64(d.Size())
		for _, m := range d.Metrics {
			metrics[m.GetName()] = append(metrics[m.GetName()], m)
		}
		servers = append(servers, r.server)
	}

	if len(metrics) == 0 {
		return servers, nil
	}

	var multi pb3.MultiFetchResponse
	for name, decoded := range metrics {
		if ce := logger.Check(zap.DebugLevel, "decoded response"); ce != nil {
			ce.Write(
				zap.String("name", name),
				zap.Any("decoded", decoded),
			)
		}

		if len(decoded) == 1 {
			if ce := logger.Check(zap.DebugLevel, "only one decoded response to merge"); ce != nil {
				ce.Write(
					zap.String("name", name),
				)
			}

			m := decoded[0]
			multi.Metrics = append(multi.Metrics, m)
			continue
		}

		// Use the metric with the highest resolution as our base
		sort.Sort(byStepTime(decoded))
		metric := decoded[0]

		z.mergeValues(&metric, decoded, stats)
		multi.Metrics = append(multi.Metrics, metric)
	}

	stats.MemoryUsage += int64(multi.Size())

	return servers, &multi
}

func (z *Zipper) mergeValues(metric *pb3.FetchResponse, decoded []pb3.FetchResponse, stats *Stats) {
	for i := range metric.Values {
		if !metric.IsAbsent[i] {
			continue
		}

		// found a missing value, look a replacement
		for j := 1; j < len(decoded); j++ {
			m := decoded[j]

			if len(m.Values) != len(metric.Values) {
				break
			}

			// found one
			if !m.IsAbsent[i] {
				metric.IsAbsent[i] = false
				metric.Values[i] = m.Values[i]
			}
		}
	}
}

func (z *Zipper) infoUnpackPB(responses []ServerResponse, stats *Stats) map[string]pb3.InfoResponse {
	logger := z.logger.With(zap.String("function", "infoUnpackPB"))

	decoded := make(map[string]pb3.InfoResponse)
	for _, r := range responses {
		if r.response == nil {
			continue
		}
		var d pb3.InfoResponse
		err := d.Unmarshal(r.response)
		if err != nil {
			err = errors.WithStack(err)
			logger.Error("error decoding protobuf response",
				zap.String("server", r.server),
				zap.String("error", fmt.Sprintf("%+v", err)),
			)

			if ce := logger.Check(zap.DebugLevel, "response hexdump"); ce != nil {
				ce.Write(
					zap.String("response", hex.Dump(r.response)),
				)
			}

			stats.InfoErrors++
			continue
		}
		if d.Name[0] == '\n' {
			var d pb3.ZipperInfoResponse
			err := d.Unmarshal(r.response)
			if err != nil {
				err = errors.WithStack(err)
				logger.Error("error decoding protobuf response",
					zap.String("server", r.server),
					zap.String("error", fmt.Sprintf("%+v", err)),
				)

				if ce := logger.Check(zap.DebugLevel, "response hexdump"); ce != nil {
					ce.Write(
						zap.String("response", hex.Dump(r.response)),
					)
				}

				stats.InfoErrors++
				continue
			}
			for _, r := range d.Responses {
				if r.Info != nil {
					decoded[r.Server] = *r.Info
				}
			}
		} else {
			decoded[r.server] = d
		}
	}

	if ce := logger.Check(zap.DebugLevel, "info request"); ce != nil {
		ce.Write(
			zap.Any("decoded_response", decoded),
		)
	}

	return decoded
}

func (z *Zipper) findUnpackPB(responses []ServerResponse, stats *Stats) ([]pb3.GlobMatch, map[string][]string) {
	logger := z.logger.With(zap.String("handler", "findUnpackPB"))

	// metric -> [server1, ... ]
	paths := make(map[string][]string)
	seen := make(map[nameLeaf]bool)

	var metrics []pb3.GlobMatch
	for _, r := range responses {
		var metric pb3.GlobResponse
		err := metric.Unmarshal(r.response)
		if err != nil {
			err = errors.WithStack(err)
			logger.Error("error decoding protobuf response",
				zap.String("server", r.server),
				zap.String("error", fmt.Sprintf("%+v", err)),
			)

			if ce := logger.Check(zap.DebugLevel, "response hexdump"); ce != nil {
				ce.Write(
					zap.String("response", hex.Dump(r.response)),
				)
			}

			stats.FindErrors += 1
			continue
		}

		for _, match := range metric.Matches {
			n := nameLeaf{match.Path, match.IsLeaf}
			_, ok := seen[n]
			if !ok {
				// we haven't seen this name yet
				// add the metric to the list of metrics to return
				metrics = append(metrics, match)
				seen[n] = true
			}
			// add the server to the list of servers that know about this metric
			p := paths[match.Path]
			p = append(p, r.server)
			paths[match.Path] = p
		}
	}

	return metrics, paths
}

func (z *Zipper) doProbe() {
	stats := &Stats{}
	logger := z.logger.With(zap.String("function", "probe"))
	ctx := util.WithUUID(context.Background())
	query := "/metrics/find/?format=protobuf&query=%2A"

	responses := z.multiGet(ctx, logger, z.backends, query, stats)

	if len(responses) == 0 {
		logger.Info("TLD Probe returned empty set")
		return
	}

	_, paths := z.findUnpackPB(responses, stats)

	z.sendStats(stats)

	incompleteResponse := false
	if len(responses) != len(z.backends) {
		incompleteResponse = true
	}

	logger.Info("TLD Probe run results",
		zap.String("carbonzipper_uuid", util.GetUUID(ctx)),
		zap.Int("paths_count", len(paths)),
		zap.Int("responses_received", len(responses)),
		zap.Int("backends", len(z.backends)),
		zap.Bool("incomplete_response", incompleteResponse),
	)

	// update our cache of which servers have which metrics
	for k, v := range paths {
		z.pathCache.Set(k, v)

		if ce := logger.Check(zap.DebugLevel, "TLD Probe"); ce != nil {
			ce.Write(
				zap.String("path", k),
				zap.Strings("servers", v),
				zap.String("carbonzipper_uuid", util.GetUUID(ctx)),
			)
		}
	}
}

func (z *Zipper) probeTlds() {
	for {
		select {
		case <-z.probeTicker.C:
			z.doProbe()
		case <-z.ProbeForce:
			z.doProbe()
		case <-z.ProbeQuit:
			z.probeTicker.Stop()
			return
		}
	}
}

func (z *Zipper) singleGet(ctx context.Context, logger *zap.Logger, uri, server string, ch chan<- ServerResponse) {
	logger = logger.With(zap.String("handler", "singleGet"))

	u, err := url.Parse(server + uri)
	if err != nil {
		if ce := logger.Check(zap.DebugLevel, "error parsing uri"); ce != nil {
			ce.Write(
				zap.String("uri", server+uri),
				zap.Error(err),
			)
		}

		ch <- ServerResponse{server: server, response: nil, err: errors.Wrap(err, "Error parsing URI")}
		return
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		if ce := logger.Check(zap.DebugLevel, "failed to create new request"); ce != nil {
			ce.Write(zap.Error(err))
		}

		ch <- ServerResponse{server: server, response: nil, err: errors.Wrap(err, "Failed to create new request")}
		return
	}
	req = util.MarshalCtx(ctx, req)

	logger = logger.With(zap.String("query", server+"/"+uri))

	z.limiter.Enter(server)
	resp, err := z.storageClient.Do(req.WithContext(ctx))
	z.limiter.Leave(server)

	if err != nil {
		if ce := logger.Check(zap.DebugLevel, "query error"); ce != nil {
			ce.Write(zap.Error(err))
		}

		ch <- ServerResponse{server: server, response: nil, err: errors.Wrap(err, "Request error")}
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// carbonsserver replies with Not Found if we request a
		// metric that it doesn't have -- makes sense
		ch <- ServerResponse{server: server, response: nil, err: nil}
		return
	}

	if resp.StatusCode != http.StatusOK {
		if ce := logger.Check(zap.DebugLevel, "bad response code"); ce != nil {
			ce.Write(zap.Int("response_code", resp.StatusCode))
		}

		ch <- ServerResponse{
			server:   server,
			response: nil,
			err:      errors.Errorf("Bad response code %d", resp.StatusCode),
		}
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		if ce := logger.Check(zap.DebugLevel, "error reading body"); ce != nil {
			ce.Write(zap.Error(err))
		}

		ch <- ServerResponse{server: server, response: nil, err: errors.Wrap(err, "Error reading body")}
		return
	}

	ch <- ServerResponse{server: server, response: body, err: nil}
}

func (z *Zipper) multiGet(ctx context.Context, logger *zap.Logger, servers []string, uri string, stats *Stats) []ServerResponse {
	logger = logger.With(
		zap.String("handler", "multiGet"),
		zap.String("uri", uri),
		zap.String("carbonapi_uuid", util.GetUUID(ctx)),
	)

	if ce := logger.Check(zap.DebugLevel, "querying servers"); ce != nil {
		ce.Write(
			zap.Strings("servers", servers),
		)
	}

	// buffered channel so the goroutines don't block on send
	ch := make(chan ServerResponse, len(servers))
	for _, server := range servers {
		go z.singleGet(ctx, logger, uri, server, ch)
	}

	responses := make([]ServerResponse, 0, len(servers))
GATHER:
	for {
		select {
		case r := <-ch:
			responses = append(responses, r)
			if len(responses) == len(servers) {
				break GATHER
			}

		case <-ctx.Done():
			break GATHER
		}
	}

	if ctx.Err() != nil {
		stats.Timeouts++
	}

	respOK := make([]ServerResponse, 0, len(servers))
	errs := make(map[string][]string)

	for _, r := range responses {
		switch t := errors.Cause(r.err).(type) {
		case nil:
			respOK = append(respOK, r)

		case *net.OpError:
			msg := netOpErrorMessage(t)
			errs[msg] = append(errs[msg], r.server)

		case *url.Error:
			var msg string
			switch s := t.Err.(type) {
			case *net.OpError:
				msg = netOpErrorMessage(s)
			default:
				msg = s.Error()
			}
			errs[msg] = append(errs[msg], r.server)

		default:
			errs[t.Error()] = append(errs[t.Error()], r.server)
		}
	}

	if len(errs) > 0 {
		es := make([]zap.Field, 0, len(errs)+1)
		es = append(es, zap.Namespace("errors"))
		for e, servers := range errs {
			es = append(es, zap.Strings(e, servers))
		}

		logger.With(es...).Warn("Errors in responses")
	}

	return respOK
}

func netOpErrorMessage(err *net.OpError) string {
	if err.Timeout() {
		return "timeout"
	}

	switch s := err.Err.(type) {
	case *net.ParseError:
		return fmt.Sprintf("net/ParseError")

	case *net.AddrError:
		return fmt.Sprintf("net/AddrError")

	case *net.UnknownNetworkError:
		return fmt.Sprintf("net/UnknownNetworkError")

	case *net.InvalidAddrError:
		return fmt.Sprintf("net/InvalidAddrError")

	case *net.DNSError:
		return s.Err

	default:
		return s.Error()
	}
}

func (z *Zipper) fetchCarbonsearchResponse(ctx context.Context, logger *zap.Logger, url string, stats *Stats) []string {
	// Send query to SearchBackend. The result is []queries for StorageBackends
	searchResponse := z.multiGet(ctx, logger, []string{z.searchBackend}, url, stats)
	m, _ := z.findUnpackPB(searchResponse, stats)

	queries := make([]string, 0, len(m))
	for _, v := range m {
		queries = append(queries, v.Path)
	}
	return queries
}

func (z *Zipper) Render(ctx context.Context, logger *zap.Logger, target string, from, until int32) (*pb3.MultiFetchResponse, *Stats, error) {
	stats := &Stats{}

	rewrite, _ := url.Parse("http://127.0.0.1/render/")

	v := url.Values{
		"target": []string{target},
		"format": []string{"protobuf"},
		"from":   []string{strconv.Itoa(int(from))},
		"until":  []string{strconv.Itoa(int(until))},
	}
	rewrite.RawQuery = v.Encode()

	var serverList []string
	var ok bool
	var responses []ServerResponse
	if z.searchConfigured && strings.HasPrefix(target, z.searchPrefix) {
		stats.SearchRequests++

		var metrics []string
		if metrics, ok = z.searchCache.Get(target); !ok || metrics == nil || len(metrics) == 0 {
			stats.SearchCacheMisses++
			findURL := &url.URL{Path: "/metrics/find/"}
			findValues := url.Values{}
			findValues.Set("format", "protobuf")
			findValues.Set("query", target)
			findURL.RawQuery = findValues.Encode()

			metrics = z.fetchCarbonsearchResponse(ctx, logger, findURL.RequestURI(), stats)
			z.searchCache.Set(target, metrics)
		} else {
			stats.SearchCacheHits++
		}

		for _, target := range metrics {
			v.Set("target", target)
			rewrite.RawQuery = v.Encode()

			// lookup the server list for this metric, or use all the servers if it's unknown
			if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
				stats.CacheMisses++
				serverList = z.backends
			} else {
				stats.CacheHits++
			}

			newResponses := z.multiGet(ctx, logger, serverList, rewrite.RequestURI(), stats)
			responses = append(responses, newResponses...)
		}
	} else {
		rewrite.RawQuery = v.Encode()

		// lookup the server list for this metric, or use all the servers if it's unknown
		if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
			stats.CacheMisses++
			serverList = z.backends
		} else {
			stats.CacheHits++
		}

		responses = z.multiGet(ctx, logger, serverList, rewrite.RequestURI(), stats)
	}

	for i := range responses {
		stats.MemoryUsage += int64(len(responses[i].response))
	}

	if len(responses) == 0 {
		return nil, stats, errors.New(errNoResponses)
	}

	servers, metrics := z.mergeResponses(responses, stats)

	if metrics == nil {
		return nil, stats, errors.New(errNoMetricsFetched)
	}

	z.pathCache.Set(target, servers)

	return metrics, stats, nil
}

func (z *Zipper) Info(ctx context.Context, logger *zap.Logger, target string) (map[string]pb3.InfoResponse, *Stats, error) {
	stats := &Stats{}
	var serverList []string
	var ok bool

	// lookup the server list for this metric, or use all the servers if it's unknown
	if serverList, ok = z.pathCache.Get(target); !ok || serverList == nil || len(serverList) == 0 {
		stats.CacheMisses++
		serverList = z.backends
	} else {
		stats.CacheHits++
	}

	rewrite, _ := url.Parse("http://127.0.0.1/info/")

	v := url.Values{
		"target": []string{target},
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	responses := z.multiGet(ctx, logger, serverList, rewrite.RequestURI(), stats)

	if len(responses) == 0 {
		stats.InfoErrors++
		return nil, stats, errors.New(errNoResponses)
	}

	infos := z.infoUnpackPB(responses, stats)
	return infos, stats, nil
}

func (z *Zipper) Find(ctx context.Context, logger *zap.Logger, query string) ([]pb3.GlobMatch, *Stats, error) {
	stats := &Stats{}
	queries := []string{query}

	rewrite, _ := url.Parse("http://127.0.0.1/metrics/find/")

	v := url.Values{
		"query":  queries,
		"format": []string{"protobuf"},
	}
	rewrite.RawQuery = v.Encode()

	if z.searchConfigured && strings.HasPrefix(query, z.searchPrefix) {
		stats.SearchRequests++
		// 'completer' requests are translated into standard Find requests with
		// a trailing '*' by graphite-web
		if strings.HasSuffix(query, "*") {
			searchCompleterResponse := z.multiGet(ctx, logger, []string{z.searchBackend}, rewrite.RequestURI(), stats)
			matches, _ := z.findUnpackPB(searchCompleterResponse, stats)
			// this is a completer request, and so we should return the set of
			// virtual metrics returned by carbonsearch verbatim, rather than trying
			// to find them on the stores
			return matches, stats, nil
		}
		var ok bool
		if queries, ok = z.searchCache.Get(query); !ok || queries == nil || len(queries) == 0 {
			stats.SearchCacheMisses++
			queries = z.fetchCarbonsearchResponse(ctx, logger, rewrite.RequestURI(), stats)
			z.searchCache.Set(query, queries)
		} else {
			stats.SearchCacheHits++
		}
	}

	var metrics []pb3.GlobMatch
	// TODO(nnuss): Rewrite the result queries to a series of brace expansions based on TLD?
	// [a.b, a.c, a.dee.eee.eff, x.y] => [ "a.{b,c,dee.eee.eff}", "x.y" ]
	// Be mindful that carbonserver's default MaxGlobs is 10
	for _, query := range queries {

		v.Set("query", query)
		rewrite.RawQuery = v.Encode()

		var tld string
		if i := strings.IndexByte(query, '.'); i > 0 {
			tld = query[:i]
		}

		// lookup tld in our map of where they live to reduce the set of
		// servers we bug with our find
		var backends []string
		var ok bool
		if backends, ok = z.pathCache.Get(tld); !ok || backends == nil || len(backends) == 0 {
			stats.CacheMisses++
			backends = z.backends
		} else {
			stats.CacheHits++
		}

		responses := z.multiGet(ctx, logger, backends, rewrite.RequestURI(), stats)

		if len(responses) == 0 {
			return nil, stats, errors.New(errNoResponses)
		}

		m, paths := z.findUnpackPB(responses, stats)
		metrics = append(metrics, m...)

		// update our cache of which servers have which metrics
		allServers := make([]string, 0)
		allServersSeen := make(map[string]struct{})
		for k, v := range paths {
			servers := make([]string, 0)
			serversSeen := make(map[string]struct{})
			for _, s := range v {
				if _, ok := serversSeen[s]; !ok {
					serversSeen[s] = struct{}{}
					servers = append(servers, s)
				}
				if _, ok := allServersSeen[s]; !ok {
					allServersSeen[s] = struct{}{}
					allServers = append(allServers, s)
				}
			}
			z.pathCache.Set(k, servers)
		}
		z.pathCache.Set(query, allServers)
	}

	return metrics, stats, nil
}
