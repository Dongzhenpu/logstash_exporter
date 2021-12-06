package main

import (
	"fmt"
	"github.com/go-kit/log/level"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/version"
	"github.com/prometheus/exporter-toolkit/web"
	"github.com/sequra/logstash_exporter/collector"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	scrapeDurations = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Namespace: collector.Namespace,
			Subsystem: "exporter",
			Name:      "scrape_duration_seconds",
			Help:      "logstash_exporter: Duration of a scrape job.",
		},
		[]string{"collector", "result"},
	)
)

// LogstashCollector collector type
type LogstashCollector struct {
	collectors map[string]collector.Collector
}

// Describe logstash metrics
func (coll LogstashCollector) Describe(ch chan<- *prometheus.Desc) {
	scrapeDurations.Describe(ch)
}

// Collect logstash metrics
func (coll LogstashCollector) Collect(ch chan<- prometheus.Metric) {
	wg := sync.WaitGroup{}
	wg.Add(len(coll.collectors))
	for name, c := range coll.collectors {
		go func(name string, c collector.Collector) {
			execute(name, c, ch)
			wg.Done()
		}(name, c)
	}
	wg.Wait()
	scrapeDurations.Collect(ch)
}

func execute(name string, c collector.Collector, ch chan<- prometheus.Metric) {
	begin := time.Now()
	err := c.Collect(ch)
	duration := time.Since(begin)
	var result string

	logger := getLogger("info", "stdout", "logfmt")
	if err != nil {
		msg := fmt.Sprintf("ERROR: %s collector failed after %fs: %s", name, duration.Seconds(), err)
		level.Debug(logger).Log("msg", msg)
		result = "error"
	} else {
		msg := fmt.Sprintf("OK: %s collector succeeded after %fs.", name, duration.Seconds())
		level.Debug(logger).Log("msg", msg)
		result = "success"
	}
	scrapeDurations.WithLabelValues(name, result).Observe(duration.Seconds())
}

func init() {
	prometheus.MustRegister(version.NewCollector("logstash_exporter"))
}

func main() {
	var (
		logstashEndpoint    = kingpin.Flag("logstash.endpoint",
			"The protocol, host and port on which logstash metrics API listens").
			Default("http://localhost:9600").String()
		exporterBindAddress = kingpin.Flag("web.listen-address",
			"Address on which to expose metrics and web interface.").
			Default(":9198").String()
		configFile = kingpin.Flag("web.config",
			"Path to config yaml file that can enable TLS or authentication.",
		).Default("").String()
	)

	kingpin.Version(version.Print("logstash_exporter"))
	kingpin.CommandLine.HelpFlag.Short('h')
	kingpin.Parse()

	logger := getLogger("info", "stdout", "logfmt")

	nodeStatsCollector, err := collector.NewNodeStatsCollector(*logstashEndpoint)
	if err != nil {
		_ = level.Error(logger).Log(
			"msg", "Cannot register a new collector",
			"err", err)
	}

	nodeInfoCollector, err := collector.NewNodeInfoCollector(*logstashEndpoint)
	if err != nil {
		_ = level.Error(logger).Log(
			"msg", "Cannot register a new collector",
			"err", err)
	}

	logstashCollector:= &LogstashCollector{
		collectors: map[string]collector.Collector{
			"node": nodeStatsCollector,
			"info": nodeInfoCollector,
		},
	}

	prometheus.MustRegister(logstashCollector)

	_ = level.Info(logger).Log("msg", "starting logstash exporter")
	_ = level.Info(logger).Log("msg", "build context")

	http.Handle("/metrics", promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/metrics", http.StatusMovedPermanently)
	})
	server := &http.Server{
		Addr: *exporterBindAddress,
	}
	_ = level.Info(logger).Log("msg", "Starting server", "listen address", *exporterBindAddress)
	if err:= web.ListenAndServe(server, *configFile, logger); err != nil {
		_ = level.Error(logger).Log(
			"msg", "Cannot start Logstash exporter",
			"err", err)
	}
}
