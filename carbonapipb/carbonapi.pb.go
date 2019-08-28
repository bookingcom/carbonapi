package carbonapipb

import (
	"net/http"
	"strings"

	"github.com/bookingcom/carbonapi/cfg"
	"github.com/bookingcom/carbonapi/util"
)

// TODO (grzkv) All this seems to be in a completely wron package

type AccessLogDetails struct {
	Handler                       string            `json:"handler,omitempty"`
	CarbonapiUuid                 string            `json:"carbonapi_uuid,omitempty"`
	Username                      string            `json:"username,omitempty"`
	Url                           string            `json:"url,omitempty"`
	PeerIp                        string            `json:"peer_ip,omitempty"`
	PeerPort                      string            `json:"peer_port,omitempty"`
	Host                          string            `json:"host,omitempty"`
	Referer                       string            `json:"referer,omitempty"`
	Format                        string            `json:"format,omitempty"`
	UseCache                      bool              `json:"use_cache,omitempty"`
	HeadersData                   map[string]string `json:"headers_data,omitempty"`
	RequestMethod                 string            `json:"request_method,omitempty"`
	Targets                       []string          `json:"targets,omitempty"`
	CacheTimeout                  int32             `json:"cache_timeout,omitempty"`
	Metrics                       []string          `json:"metrics,omitempty"`
	HaveNonFatalErrors            bool              `json:"have_non_fatal_errors,omitempty"`
	Runtime                       float64           `json:"runtime,omitempty"`
	HttpCode                      int32             `json:"http_code"`
	CarbonzipperResponseSizeBytes int64             `json:"carbonzipper_response_size_bytes,omitempty"`
	CarbonapiResponseSizeBytes    int64             `json:"carbonapi_response_size_bytes,omitempty"`
	Reason                        string            `json:"reason,omitempty"`
	SendGlobs                     bool              `json:"send_globs"`
	From                          int32             `json:"from,omitempty"`
	Until                         int32             `json:"until,omitempty"`
	Tz                            string            `json:"tz,omitempty"`
	FromRaw                       string            `json:"from_raw,omitempty"`
	UntilRaw                      string            `json:"until_raw,omitempty"`
	Path                          string            `json:"path,omitempty"`
	Uri                           string            `json:"uri,omitempty"`
	FromCache                     bool              `json:"from_cache"`
	ZipperRequests                int64             `json:"zipper_requests,omitempty"`
	TotalMetricCount              int64             `json:"total_metric_count"`
}

func splitAddr(addr string) (string, string) {
	tmp := strings.Split(addr, ":")
	if len(tmp) < 1 {
		return "unknown", "unknown"
	}
	if len(tmp) == 1 {
		return tmp[0], ""
	}
	return tmp[0], tmp[1]
}

func NewAccessLogDetails(r *http.Request, handler string, config *cfg.API) AccessLogDetails {
	username, _, _ := r.BasicAuth()
	srcIP, srcPort := splitAddr(r.RemoteAddr)

	return AccessLogDetails{
		Handler:       handler,
		Username:      username,
		CarbonapiUuid: util.GetUUID(r.Context()),
		HeadersData:   getHeadersData(r, config.HeadersToLog),
		Url:           r.URL.String(),
		PeerIp:        srcIP,
		PeerPort:      srcPort,
		Host:          r.Host,
		Path:          r.URL.Path,
		Referer:       r.Referer(),
		// TODO (grzkv) Do we need this?
		Uri: r.RequestURI,
		// 0 means the code is not specified
		HttpCode: 0,
	}
}

func getHeadersData(r *http.Request, headersToLog []string) map[string]string {
	headerData := make(map[string]string)
	for _, headerToLog := range headersToLog {
		headerValue := r.Header.Get(headerToLog)
		if headerValue != "" {
			headerData[headerToLog] = headerValue
		}
	}
	return headerData
}
