package test

import "os"

const (
	EnvTestSpannerProject  = "GOTAFACE_TEST_SPANNER_PROJECT"
	EnvTestSpannerInstance = "GOTAFACE_TEST_SPANNER_INSTANCE"
)

type EnvSpanner struct {
	Project  string
	Instance string
}

func GetEnvSpanner() EnvSpanner {
	return EnvSpanner{
		Project:  os.Getenv(EnvTestSpannerProject),
		Instance: os.Getenv(EnvTestSpannerInstance),
	}
}
