package main

// Inspired by the noaa firehose sample script
// https://github.com/cloudfoundry/noaa/blob/master/firehose_sample/main.go

import (
	"crypto/tls"
	"fmt"
	"regexp"
	"strings"

	"github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-community/gogobosh"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/caching"
	"github.com/pivotal-cf/graphite-nozzle/logger"
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
	appsDomains       = kingpin.Flag("apps-domains", "List of CF apps domains, to filter out HttpStartStop metrics").Strings()
)

type CfClientTokenRefresh struct {
	cfClient *cfclient.Client
}

func (ct *CfClientTokenRefresh) RefreshAuthToken() (string, error) {
	return ct.cfClient.GetToken()
}

func main() {
	kingpin.Parse()

	var domainsRegexp []*regexp.Regexp
	for _, appsDomain := range *appsDomains {
		domainRegexpRaw := fmt.Sprintf(`^.*%s(?:_\d{2,5})?$`, strings.Replace(appsDomain, ".", "_", -1))
		domainRegexp, err := regexp.Compile(domainRegexpRaw)
		if err != nil {
			logger.Error.Printf("Failed to create HttpStartStop filter rule %s: %s", domainRegexpRaw, err)
			panic(err)
		}
		domainsRegexp = append(domainsRegexp, domainRegexp)
	}

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
	logger.Info.Println("Start filling app/space/org cache.")
	apps := cachingClient.GetAllApp()
	logger.Info.Printf("Done filling cache! Found [%d] Apps", len(apps))
	logger.Info.Println("Start filling Jobs cache.")
	jobs := cachingClient.GetAllJobs()
	logger.Info.Printf("Done filling cache! Found [%d] Jobs", len(jobs))
	//Let's start the polling goRoutine
	cachingClient.PerformPoollingCaching(*ccTickerTime)
	cachingClient.PerformBoshPoolingCaching(*boshTickerTime)

	httpStartStopProcessor := processors.NewHttpStartStopProcessor(cachingClient, domainsRegexp)
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
			logger.Error.Printf("Got error from firehose: %v", err.Error())
		}
	}()

	var prefix string
	var internalPrefix string
	var jobName string
	var jobIndexStr string
	var jobIndex int
	var jobOrigin string
	var deploymentName string

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
			continue
		}

		if proc_err != nil {
			logger.Error.Printf("Processing Error: %v", proc_err.Error())
			continue
		}

		if *prefixJob {
			deploymentName = msg.GetDeployment()
			jobName = msg.GetJob()
			jobIndexStr = msg.GetIndex()
			jobIndex = cachingClient.GetJobInfoCache(jobIndexStr).Index

			// skip metric with empty name
			if jobName == "" {
				logger.Error.Println("Got a job without name.")
				continue
				// skip metric with empty index
			} else if jobIndexStr == "" {
				logger.Error.Printf("Job %v has came with bad index.\n", jobName)
				continue
			}
			// TODO: firehose and bosh use different names for deployment eg cf-rabbit vs p-rabbitmq-8ec01d6a62ee6bf4452b
			// for now we can live with this dirty hack
			jobOrigin = msg.GetOrigin()
			if jobOrigin == "bosh-hm-forwarder" && deploymentName != "bosh-hm-forwarder" {
				internalPrefix = "bosh."
			} else {
				internalPrefix = "cf."
			}

			deploymentName = strings.Replace(deploymentName, ".", "_", -1)
			jobName = strings.Replace(jobName, ".", "_", -1)
			prefix = internalPrefix + deploymentName + "." + jobName + "." + fmt.Sprintf("%d", jobIndex)
		}
		for _, metric := range processedMetrics {
			if !*debug {
				metric.Send(sender, prefix)
			} else {
				logger.Debug.Println(prefix, metric)
			}
		}

		processedMetrics = nil
	}
}
