package data

type MetricCollection map[string]interface{}

type MetricsProvider interface {
	Metrics() MetricCollection
}
