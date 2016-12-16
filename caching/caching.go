package caching

import (
	scaching "github.com/cloudfoundry-community/firehose-to-syslog/caching"
	"time"
)

type Job struct {
	Name  string
	ID    string
	Index int
}

type Caching interface {
	scaching.Caching
	PerformBoshPoolingCaching(time.Duration)
	GetAllJobs() []Job
	GetJobInfo(string) Job
	GetJobInfoCache(string) Job
}
