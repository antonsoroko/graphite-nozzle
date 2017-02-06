package main

// Inspired by the noaa firehose sample script
// https://github.com/cloudfoundry/noaa/blob/master/firehose_sample/main.go

import (
	"crypto/tls"
	"fmt"
	"os"
	"strings"

	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-community/gogobosh"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/caching"
	"github.com/pivotal-cf/graphite-nozzle/metrics"
	"github.com/pivotal-cf/graphite-nozzle/processors"
	"github.com/quipo/statsd"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	dopplerEndpoint   = kingpin.Flag("doppler-endpoint", "Doppler endpoint").Default("wss://doppler.10.244.0.34.xip.io:443").OverrideDefaultFromEnvar("DOPPLER_ENDPOINT").String()
	apiEndpoint       = kingpin.Flag("api-endpoint", "API endpoint").Default("https://api.10.244.0.34.xip.io").OverrideDefaultFromEnvar("API_ENDPOINT").String()
	boshEndpoint      = kingpin.Flag("bosh-endpoint", "BOSH endpoint").Default("https://192.168.50.4:25555").OverrideDefaultFromEnvar("BOSH_ENDPOINT").String()
	subscriptionId    = kingpin.Flag("subscription-id", "Id for the subscription.").Default("firehose").OverrideDefaultFromEnvar("SUBSCRIPTION_ID").String()
	statsdEndpoint    = kingpin.Flag("statsd-endpoint", "Statsd endpoint").Default("10.244.11.2:8125").OverrideDefaultFromEnvar("STATSD_ENDPOINT").String()
	statsdPrefix      = kingpin.Flag("statsd-prefix", "Statsd prefix").Default("mycf.").OverrideDefaultFromEnvar("STATSD_PREFIX").String()
	prefixJob         = kingpin.Flag("prefix-job", "Prefix metric names with job.index").Default("false").OverrideDefaultFromEnvar("PREFIX_JOB").Bool()
	username          = kingpin.Flag("username", "Firehose username.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_USERNAME").String()
	password          = kingpin.Flag("password", "Firehose password.").Default("admin").OverrideDefaultFromEnvar("FIREHOSE_PASSWORD").String()
	skipSSLValidation = kingpin.Flag("skip-ssl-validation", "Please don't").Default("false").OverrideDefaultFromEnvar("SKIP_SSL_VALIDATION").Bool()
	debug             = kingpin.Flag("debug", "Enable debug mode. This disables forwarding to statsd and prints to stdout").Default("false").OverrideDefaultFromEnvar("DEBUG").Bool()
	boltDatabasePath  = kingpin.Flag("boltdb-path", "Bolt Database path").Default("my.db").OverrideDefaultFromEnvar("BOLTDB_PATH").String()
	boshUsername      = kingpin.Flag("bosh-username", "BOSH username.").Default("admin").OverrideDefaultFromEnvar("BOSH_USERNAME").String()
	boshPassword      = kingpin.Flag("bosh-password", "BOSH password.").Default("admin").OverrideDefaultFromEnvar("BOSH_PASSWORD").String()
	ccTickerTime      = kingpin.Flag("cc-pull-time", "CloudController Polling time in sec").Default("60s").OverrideDefaultFromEnvar("CF_PULL_TIME").Duration()
	boshTickerTime    = kingpin.Flag("bosh-pull-time", "BOSH Polling time in sec").Default("600s").OverrideDefaultFromEnvar("BOSH_PULL_TIME").Duration()
)

type CfClientTokenRefresh struct {
	cfClient *cfclient.Client
}

func (ct *CfClientTokenRefresh) RefreshAuthToken() (string, error) {
	return ct.cfClient.GetToken()
}

func main() {
	kingpin.Parse()

	c := cfclient.Config{
		ApiAddress:        *apiEndpoint,
		Username:          *username,
		Password:          *password,
		SkipSslValidation: *skipSSLValidation,
	}
	cfClient, _ := cfclient.NewClient(&c)

	consumer := consumer.New(*dopplerEndpoint, &tls.Config{InsecureSkipVerify: *skipSSLValidation}, nil)
	refresher := CfClientTokenRefresh{cfClient: cfClient}
	consumer.RefreshTokenFrom(&refresher)

	boshConfig := gogobosh.DefaultConfig()
	boshConfig.BOSHAddress = *boshEndpoint
	boshConfig.Username = *boshUsername
	boshConfig.Password = *boshPassword
	gogoboshClient, _ := gogobosh.NewClient(boshConfig)

	//Creating Caching
	cachingClient := caching.NewCachingBolt(cfClient, *boltDatabasePath, gogoboshClient, *debug)
	cachingClient.CreateBucket()
	//Let's Update the database the first time
	fmt.Println("Start filling app/space/org cache.")
	apps := cachingClient.GetAllApp()
	fmt.Printf("Done filling cache! Found [%d] Apps\n", len(apps))
	fmt.Println("Start filling Jobs cache.")
	jobs := cachingClient.GetAllJobs()
	fmt.Printf("Done filling cache! Found [%d] Jobs\n", len(jobs))
	//Let's start the polling goRoutine
	cachingClient.PerformPoollingCaching(*ccTickerTime)
	cachingClient.PerformBoshPoolingCaching(*boshTickerTime)

	httpStartStopProcessor := processors.NewHttpStartStopProcessor(cachingClient)
	valueMetricProcessor := processors.NewValueMetricProcessor()
	counterProcessor := processors.NewCounterProcessor()
	containerMetricProcessor := processors.NewContainerMetricProcessor(cachingClient)

	sender := statsd.NewStatsdClient(*statsdEndpoint, *statsdPrefix)
	sender.CreateSocket()

	var processedMetrics []metrics.Metric
	var proc_err error

	msgChan, errorChan := consumer.Firehose(*subscriptionId, "")

	go func() {
		for err := range errorChan {
			fmt.Fprintf(os.Stderr, "%v\n", err.Error())
		}
	}()

	for msg := range msgChan {
		eventType := msg.GetEventType()

		// graphite-nozzle can handle CounterEvent, ContainerMetric,
		// HttpStartStop and ValueMetric events
		switch eventType {
		case events.Envelope_ContainerMetric:
			processedMetrics, proc_err = containerMetricProcessor.Process(msg)
		case events.Envelope_CounterEvent:
			processedMetrics, proc_err = counterProcessor.Process(msg)
		case events.Envelope_HttpStartStop:
			processedMetrics, proc_err = httpStartStopProcessor.Process(msg)
		case events.Envelope_ValueMetric:
			processedMetrics, proc_err = valueMetricProcessor.Process(msg)
		default:
			// do nothing
		}

		if proc_err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", proc_err.Error())
			continue
		}

		for _, metric := range processedMetrics {
			var prefix string
			var index int
			if *prefixJob {
				index = cachingClient.GetJobInfoCache(msg.GetIndex()).Index
				jobName := msg.GetJob()
				jobName = strings.Replace(jobName, ".", "_", -1)
				prefix = jobName + "." + fmt.Sprintf("%d", index)
			}
			metric.Send(sender, prefix)
			if *debug {
				fmt.Println(prefix, metric)
			}
		}

		processedMetrics = nil
	}
}
