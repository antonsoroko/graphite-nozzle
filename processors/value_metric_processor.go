package processors

import (
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/metrics"
	"strings"
)

type ValueMetricProcessor struct{}

func NewValueMetricProcessor() *ValueMetricProcessor {
	return &ValueMetricProcessor{}
}

func (p *ValueMetricProcessor) Process(e *events.Envelope) ([]metrics.Metric, error) {
	processedMetrics := make([]metrics.Metric, 1)
	valueMetricEvent := e.GetValueMetric()

	processedMetrics[0] = p.ProcessValueMetric(valueMetricEvent, e.GetOrigin())

	return processedMetrics, nil
}

func (p *ValueMetricProcessor) ProcessValueMetric(event *events.ValueMetric, origin string) *metrics.FGaugeMetric {
	statPrefix := "ops." + origin + "."
	valueMetricName := event.GetName()
	if origin == "gorouter" && valueMetricName == "latency" {
		valueMetricName += ".all"
	}
	// rabbit root vhost
	if origin == "p-rabbitmq" {
		valueMetricName = strings.Replace(valueMetricName, "///", "/root/", -1)
		valueMetricName = strings.Replace(valueMetricName, ".", "_", -1)
		// e.g. exchange name /p-rabbitmq/rabbitmq/queues/944...b78/spring.cloud.broker.5.update.replies/consumers
	}
	// rabbit and redis use / as delimeter
	valueMetricName = strings.Replace(valueMetricName, "/", ".", -1)
	valueMetricName = strings.Trim(valueMetricName, ".")

	stat := statPrefix + valueMetricName
	metric := metrics.NewFGaugeMetric(stat, event.GetValue())

	return metric
}
