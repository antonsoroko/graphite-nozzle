package processors

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/metrics"
	"regexp"
	"strconv"
	"strings"
)

type HttpStartStopProcessor struct {
	CachingClient caching.Caching
	DomainsRegexp []*regexp.Regexp
}

func NewHttpStartStopProcessor(caching caching.Caching, domainsRegexp []*regexp.Regexp) *HttpStartStopProcessor {
	return &HttpStartStopProcessor{CachingClient: caching, DomainsRegexp: domainsRegexp}
}

func (p *HttpStartStopProcessor) Process(e *events.Envelope) (processedMetrics []metrics.Metric, err error) {

	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("Unknown error")
			}
			processedMetrics = nil
		}
	}()

	httpStartStopEvent := e.GetHttpStartStop()

	if p.DomainsRegexp != nil {
		hostname := p.parseEventUri(httpStartStopEvent.GetUri())
		var passed bool = false
		for _, domainRegexp := range p.DomainsRegexp {
			if domainRegexp.MatchString(hostname) {
				passed = true
				break
			}
		}
		if !passed {
			panic(errors.New("Metric from bad domain: " + hostname))
		}
	}

	if httpStartStopEvent.GetApplicationId() != nil {
		processedMetrics = make([]metrics.Metric, 8)
	} else {
		processedMetrics = make([]metrics.Metric, 4)
	}

	processedMetrics[0] = p.ProcessHttpStartStopResponseTime(httpStartStopEvent)
	processedMetrics[1] = p.ProcessHttpStartStopStatusCodeCount(httpStartStopEvent)
	processedMetrics[2] = p.ProcessHttpStartStopHttpErrorCount(httpStartStopEvent)
	processedMetrics[3] = p.ProcessHttpStartStopHttpRequestCount(httpStartStopEvent)

	if httpStartStopEvent.GetApplicationId() != nil {
		processedMetrics[4] = p.ProcessHttpStartStopResponseTimeForApp(httpStartStopEvent)
		processedMetrics[5] = p.ProcessHttpStartStopStatusCodeCountForApp(httpStartStopEvent)
		processedMetrics[6] = p.ProcessHttpStartStopHttpErrorCountForApp(httpStartStopEvent)
		processedMetrics[7] = p.ProcessHttpStartStopHttpRequestCountForApp(httpStartStopEvent)
	}

	return
}

//we want to be able to parse events whether they contain the scheme
//element in their uri field or not
func (p *HttpStartStopProcessor) parseEventUri(uri string) string {

	hostname := ""

	//we first remove the scheme
	if strings.Contains(uri, "://") {
		uri = strings.Split(uri, "://")[1]
	}

	//and then proceed with extracting the hostname
	hostname = strings.Split(uri, "/")[0]
	hostname = strings.Replace(hostname, ".", "_", -1)
	hostname = strings.Replace(hostname, ":", "_", -1)

	if !(len(hostname) > 0) {
		panic(errors.New("Hostname cannot be extracted from Event uri: " + uri))
	}

	return hostname
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopResponseTime(event *events.HttpStartStop) *metrics.TimingMetric {
	// TODO: need to distinguish response times by event.GetPeerType (Client/Server)
	statPrefix := "http.responsetimes."
	hostname := p.parseEventUri(event.GetUri())
	peerType := event.GetPeerType().String()
	stat := statPrefix + hostname + "." + peerType

	startTimestamp := event.GetStartTimestamp()
	stopTimestamp := event.GetStopTimestamp()
	durationNanos := stopTimestamp - startTimestamp
	durationMillis := durationNanos / 1000000 // NB: loss of precision here
	metric := metrics.NewTimingMetric(stat, durationMillis)

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopResponseTimeForApp(event *events.HttpStartStop) *metrics.TimingMetric {
	// TODO: need to distinguish response times by event.GetPeerType (Client/Server)
	startTimestamp := event.GetStartTimestamp()
	stopTimestamp := event.GetStopTimestamp()
	durationNanos := stopTimestamp - startTimestamp
	durationMillis := durationNanos / 1000000 // NB: loss of precision here

	appID := FormatUUID(event.GetApplicationId())
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	appName = strings.Replace(appName, ".", "_", -1)
	spaceName := appInfo.SpaceName
	spaceName = strings.Replace(spaceName, ".", "_", -1)
	orgName := appInfo.OrgName
	orgName = strings.Replace(orgName, ".", "_", -1)
	instanceIndex := strconv.Itoa(int(event.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".responsetimes"
	metric := metrics.NewTimingMetric(stat, durationMillis)

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopStatusCodeCount(event *events.HttpStartStop) *metrics.CounterMetric {
	statPrefix := "http.statuscodes."
	hostname := p.parseEventUri(event.GetUri())
	stat := statPrefix + hostname + "." + strconv.Itoa(int(event.GetStatusCode()))

	metric := metrics.NewCounterMetric(stat, isPeer(event))

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopStatusCodeCountForApp(event *events.HttpStartStop) *metrics.CounterMetric {
	appID := FormatUUID(event.GetApplicationId())
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	appName = strings.Replace(appName, ".", "_", -1)
	spaceName := appInfo.SpaceName
	spaceName = strings.Replace(spaceName, ".", "_", -1)
	orgName := appInfo.OrgName
	orgName = strings.Replace(orgName, ".", "_", -1)
	// TODO: apparently GetInstanceIndex always is nil, so we need to find a way to convert
	// GetInstanceId (UUID) to Index (int)
	instanceIndex := strconv.Itoa(int(event.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".statuscodes." + strconv.Itoa(int(event.GetStatusCode()))
	metric := metrics.NewCounterMetric(stat, isPeer(event))

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopHttpErrorCount(event *events.HttpStartStop) *metrics.CounterMetric {
	var incrementValue int64

	statPrefix := "http.errors."
	hostname := p.parseEventUri(event.GetUri())
	stat := statPrefix + hostname

	if 299 < event.GetStatusCode() && 1 == isPeer(event) {
		incrementValue = 1
	} else {
		incrementValue = 0
	}

	metric := metrics.NewCounterMetric(stat, incrementValue)

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopHttpErrorCountForApp(event *events.HttpStartStop) *metrics.CounterMetric {
	var incrementValue int64

	if 299 < event.GetStatusCode() && 1 == isPeer(event) {
		incrementValue = 1
	} else {
		incrementValue = 0
	}

	appID := FormatUUID(event.GetApplicationId())
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	appName = strings.Replace(appName, ".", "_", -1)
	spaceName := appInfo.SpaceName
	spaceName = strings.Replace(spaceName, ".", "_", -1)
	orgName := appInfo.OrgName
	orgName = strings.Replace(orgName, ".", "_", -1)
	// TODO: apparently GetInstanceIndex always is nil, so we need to find a way to convert
	// GetInstanceId (UUID) to Index (int)
	instanceIndex := strconv.Itoa(int(event.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".errors"
	metric := metrics.NewCounterMetric(stat, incrementValue)

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopHttpRequestCount(event *events.HttpStartStop) *metrics.CounterMetric {
	statPrefix := "http.requests."
	hostname := p.parseEventUri(event.GetUri())
	stat := statPrefix + hostname
	metric := metrics.NewCounterMetric(stat, isPeer(event))

	return metric
}

func (p *HttpStartStopProcessor) ProcessHttpStartStopHttpRequestCountForApp(event *events.HttpStartStop) *metrics.CounterMetric {
	appID := FormatUUID(event.GetApplicationId())
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	appName = strings.Replace(appName, ".", "_", -1)
	spaceName := appInfo.SpaceName
	spaceName = strings.Replace(spaceName, ".", "_", -1)
	orgName := appInfo.OrgName
	orgName = strings.Replace(orgName, ".", "_", -1)
	// TODO: apparently GetInstanceIndex always is nil, so we need to find a way to convert
	// GetInstanceId (UUID) to Index (int)
	instanceIndex := strconv.Itoa(int(event.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".requests"
	metric := metrics.NewCounterMetric(stat, isPeer(event))

	return metric
}

func isPeer(event *events.HttpStartStop) int64 {
	if event.GetPeerType() == events.PeerType_Client {
		return 1
	} else {
		return 0
	}
}

func FormatUUID(uuid *events.UUID) string {
	if uuid == nil {
		return ""
	}
	var uuidBytes [16]byte
	binary.LittleEndian.PutUint64(uuidBytes[:8], uuid.GetLow())
	binary.LittleEndian.PutUint64(uuidBytes[8:], uuid.GetHigh())
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuidBytes[0:4], uuidBytes[4:6], uuidBytes[6:8], uuidBytes[8:10], uuidBytes[10:])
}
