package gaufre

import (
	"sync"
	"time"

	"github.com/nirosys/gaufre/data"

	log "github.com/sirupsen/logrus"
)

type RuntimeMetrics struct {
	MeanToCompleteSecs float64 `json:"average_complete_sec"`
	NumberOfFlows      uint    `json:"num_flows"`
	NumberOfErrors     uint    `json:"num_errors"`
	FlowsPerSecond     float64 `json:"flows_per_sec"`
	// TODO: Packets Per Second

	total_time_sec        float64
	flows                 map[string]time.Time
	metricsLock           sync.Mutex
	last_flow_end_time    time.Time
	first_flow_start_time time.Time
}

func NewRuntimeMetrics() *RuntimeMetrics {
	return &RuntimeMetrics{
		MeanToCompleteSecs: 0.0,
		NumberOfFlows:      0,
		NumberOfErrors:     0,
		total_time_sec:     0.0,
		flows:              make(map[string]time.Time),
	}
}

func (r *RuntimeMetrics) Metrics() data.MetricCollection {
	r.metricsLock.Lock()
	defer r.metricsLock.Unlock()

	metrics := map[string]interface{}{
		"num_flows":         r.NumberOfFlows,
		"num_errors":        r.NumberOfErrors,
		"mean_duration_sec": r.MeanToCompleteSecs,
		"in_flight":         len(r.flows),
	}
	r.MeanToCompleteSecs = 0.0
	r.NumberOfFlows = 0
	r.NumberOfErrors = 0
	r.total_time_sec = 0.0
	return metrics
}

func (r *RuntimeMetrics) FlowBegin(flowid string) {
	log := log.WithField("op", "gaufre:metrics.flowbegin")

	r.metricsLock.Lock()
	defer r.metricsLock.Unlock()

	if len(flowid) > 0 {
		if v, ok := r.flows[flowid]; ok {
			log.WithFields(map[string]interface{}{
				"flowid":     flowid,
				"start_time": v,
			}).Error("flow ID already being tracked")
		}
		r.flows[flowid] = time.Now()
	} else {
		log.WithField("flowid", flowid).Error("unable to add flow")
	}
}

func (r *RuntimeMetrics) FlowEnd(flowid string) {
	log := log.WithField("op", "gaufre:metrics.flowend")
	end := time.Now()

	r.metricsLock.Lock()
	defer r.metricsLock.Unlock()

	start, ok := r.flows[flowid]
	if !ok {
		log.WithField("flowid", flowid).Error("flow ending without record of start")
		r.NumberOfErrors += 1
		return
	}
	flowdur := end.Sub(start)
	r.total_time_sec += flowdur.Seconds()
	r.NumberOfFlows += 1
	r.MeanToCompleteSecs = r.total_time_sec / float64(r.NumberOfFlows)
	r.last_flow_end_time = time.Now()
	delete(r.flows, flowid)
}
