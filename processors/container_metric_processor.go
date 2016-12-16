package processors

import (
	"github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf/graphite-nozzle/metrics"
	"strconv"
)

type ContainerMetricProcessor struct {
	CachingClient caching.Caching
}

func NewContainerMetricProcessor(caching caching.Caching) *ContainerMetricProcessor {
	return &ContainerMetricProcessor{CachingClient: caching}
}

func (p *ContainerMetricProcessor) Process(e *events.Envelope) ([]metrics.Metric, error) {
	processedMetrics := make([]metrics.Metric, 5)
	containerMetricEvent := e.GetContainerMetric()

	processedMetrics[0] = metrics.Metric(p.ProcessContainerMetricCPU(containerMetricEvent))
	processedMetrics[1] = metrics.Metric(p.ProcessContainerMetricMemory(containerMetricEvent))
	processedMetrics[2] = metrics.Metric(p.ProcessContainerMetricMemoryQuota(containerMetricEvent))
	processedMetrics[3] = metrics.Metric(p.ProcessContainerMetricDisk(containerMetricEvent))
	processedMetrics[4] = metrics.Metric(p.ProcessContainerMetricDiskQuota(containerMetricEvent))

	return processedMetrics, nil
}

func (p *ContainerMetricProcessor) ProcessContainerMetricCPU(e *events.ContainerMetric) metrics.FGaugeMetric {
	appID := e.GetApplicationId()
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	spaceName := appInfo.SpaceName
	orgName := appInfo.OrgName
	instanceIndex := strconv.Itoa(int(e.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".cpu"
	metric := metrics.NewFGaugeMetric(stat, float64(e.GetCpuPercentage()))

	return *metric
}

func (p *ContainerMetricProcessor) ProcessContainerMetricMemory(e *events.ContainerMetric) metrics.GaugeMetric {
	appID := e.GetApplicationId()
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	spaceName := appInfo.SpaceName
	orgName := appInfo.OrgName
	instanceIndex := strconv.Itoa(int(e.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".memoryBytes"
	metric := metrics.NewGaugeMetric(stat, int64(e.GetMemoryBytes()))

	return *metric
}

func (p *ContainerMetricProcessor) ProcessContainerMetricMemoryQuota(e *events.ContainerMetric) metrics.GaugeMetric {
	appID := e.GetApplicationId()
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	spaceName := appInfo.SpaceName
	orgName := appInfo.OrgName
	instanceIndex := strconv.Itoa(int(e.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".memoryBytesQuota"
	metric := metrics.NewGaugeMetric(stat, int64(e.GetMemoryBytesQuota()))

	return *metric
}

func (p *ContainerMetricProcessor) ProcessContainerMetricDisk(e *events.ContainerMetric) metrics.GaugeMetric {
	appID := e.GetApplicationId()
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	spaceName := appInfo.SpaceName
	orgName := appInfo.OrgName
	instanceIndex := strconv.Itoa(int(e.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".diskBytes"
	metric := metrics.NewGaugeMetric(stat, int64(e.GetDiskBytes()))

	return *metric
}

func (p *ContainerMetricProcessor) ProcessContainerMetricDiskQuota(e *events.ContainerMetric) metrics.GaugeMetric {
	appID := e.GetApplicationId()
	appInfo := p.CachingClient.GetAppInfoCache(appID)
	appName := appInfo.Name
	spaceName := appInfo.SpaceName
	orgName := appInfo.OrgName
	instanceIndex := strconv.Itoa(int(e.GetInstanceIndex()))

	stat := "apps." + orgName + "." + spaceName + "." + appName + "." + instanceIndex + ".diskBytesQuota"
	metric := metrics.NewGaugeMetric(stat, int64(e.GetDiskBytesQuota()))

	return *metric
}
