package processors

import (
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/metrics"
	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"strconv"
	"strings"
)

type ValueMetricProcessor struct{
	CachingClient caching.Caching
}

func NewValueMetricProcessor(caching caching.Caching) *ValueMetricProcessor {
	return &ValueMetricProcessor{CachingClient: caching}
}

func (p *ValueMetricProcessor) Process(e *events.Envelope) ([]metrics.Metric, error) {
	processedMetrics := make([]metrics.Metric, 1)
	valueMetricEvent := e.GetValueMetric()

	processedMetrics[0] = p.ProcessValueMetric(valueMetricEvent, e.GetOrigin())

	return processedMetrics, nil
}

func (p *ValueMetricProcessor) ProcessValueMetric(event *events.ValueMetric, origin string) *metrics.FGaugeMetric {
	var stat string
	statPrefix := "ops." + origin + "."
	valueMetricName := event.GetName()
	if origin == "gorouter" && valueMetricName == "latency" {
		valueMetricName += ".all"
	}
	if origin == "jmxtrans" {
		metricFields := strings.Fields(valueMetricName)
		//is <nil> is good for metric? for Send? I see - no. We will check for nil in main.
		if len(metricFields) < 3 {
			return nil
		}
		_, err := strconv.Atoi(metricFields[1])
		if err != nil {
			return nil
		}
		appID := metricFields[0]
		appInfo := p.CachingClient.GetAppInfoCache(appID)
		appName := appInfo.Name
		spaceName := appInfo.SpaceName
		orgName := appInfo.OrgName
		instanceIndex := metricFields[1]

		stat = "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".cpu"
	} else {
		stat = statPrefix + valueMetricName
	}
	// TODO: NEED TO CHECK GetUnit() and then choose appropriate metric type !!!
	metric := metrics.NewFGaugeMetric(stat, event.GetValue())

	return metric
}
