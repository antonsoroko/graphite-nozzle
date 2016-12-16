package caching

import (
	"fmt"
	"github.com/boltdb/bolt"
	scaching "github.com/cloudfoundry-community/firehose-to-syslog/caching"
	cfClient "github.com/cloudfoundry-community/go-cfclient"
	"github.com/cloudfoundry-community/gogobosh"
	json "github.com/mailru/easyjson"
	"time"
)

type CachingBolt struct {
	*scaching.CachingBolt
	GogoboshClient *gogobosh.Client
	debug          bool
}

func NewCachingBolt(gcfClientSet *cfClient.Client, boltDatabasePath string, gogoboshClient *gogobosh.Client, debug bool) Caching {
	sCachingBolt := scaching.NewCachingBolt(gcfClientSet, boltDatabasePath).(*scaching.CachingBolt)
	return &CachingBolt{
		sCachingBolt,
		gogoboshClient,
		debug,
	}
}

func (c *CachingBolt) CreateBucket() {
	c.CachingBolt.CreateBucket()
	c.Appdb.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte("JobBucket"))
		if err != nil {
			return fmt.Errorf("Create bucket: %s", err)
		}
		return nil

	})

}

func (c *CachingBolt) PerformBoshPoolingCaching(boshTickerTime time.Duration) {
	// Ticker Pooling the CC every X sec
	ccPooling := time.NewTicker(boshTickerTime)

	var jobs []Job
	go func() {
		for range ccPooling.C {
			jobs = c.GetAllJobs()
		}
	}()

}

func (c *CachingBolt) fillDatabase(listJobs []Job) {
	for _, job := range listJobs {
		c.Appdb.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte("JobBucket"))
			if err != nil {
				return fmt.Errorf("Create bucket: %s", err)
			}

			serialize, err := json.Marshal(job)

			if err != nil {
				return fmt.Errorf("Error Marshaling data: %s", err)
			}
			err = b.Put([]byte(job.ID), serialize)

			if err != nil {
				return fmt.Errorf("Error inserting data: %s", err)
			}
			return nil
		})

	}

}

func (c *CachingBolt) GetAllJobs() []Job {
	fmt.Println("Retrieving Jobs for Cache...")
	var jobs []Job

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("Recovered in caching.GetAllJob()", r)
		}
	}()

	deployments, err := c.GogoboshClient.GetDeployments()
	if err != nil {
		return jobs
	}

	for _, deployment := range deployments {
		vms, err := c.GogoboshClient.GetDeploymentVMsShort(deployment.Name)
		if err != nil {
			return jobs
		}
		for _, vm := range vms {
			fmt.Printf("Job [%s.%d] Found...\n", vm.Name(), vm.Index)
			jobs = append(jobs, Job{vm.Name(), vm.ID, vm.Index})
		}
	}

	c.fillDatabase(jobs)
	fmt.Sprintf("Found [%d] Jobs!", len(jobs))

	return jobs
}

//func (c *CachingBolt) GetJobByGuid(jobID string) []Job {
//	var jobs []Job
//	job, err := c.GcfClient.AppByGuid(jobID)
//	if err != nil {
//		return jobs
//	}
//	jobs = append(jobs, Job{job.JobName, job.ID, job.Index})
//	c.fillDatabase(jobs)
//	return jobs
//
//}

func (c *CachingBolt) GetJobInfo(jobID string) Job {

	var d []byte
	var job Job
	c.Appdb.View(func(tx *bolt.Tx) error {
		if c.debug {
			//fmt.Printf("Looking for Job %s in Cache!\n", jobID)
		}
		b := tx.Bucket([]byte("JobBucket"))
		d = b.Get([]byte(jobID))
		return nil
	})
	err := json.Unmarshal([]byte(d), &job)
	if err != nil {
		return Job{}
	}
	return job
}

func (c *CachingBolt) GetJobInfoCache(jobID string) Job {
	if job := c.GetJobInfo(jobID); job.Name != "" {
		return job
	} else {
		c.GetAllJobs()
	}
	return c.GetJobInfo(jobID)
}
