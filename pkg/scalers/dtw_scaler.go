package scalers

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-logr/logr"
	//	metrics "github.com/rcrowley/go-metrics"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/metrics/pkg/apis/external_metrics"
	//	logf "sigs.k8s.io/controller-runtime/pkg/log"

	//	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	kedautil "github.com/kedacore/keda/v2/pkg/util"
	//v2beta2 "k8s.io/api/autoscaling/v2beta2"
	//"k8s.io/apimachinery/pkg/api/resource"
	//etav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	//"k8s.io/apimachinery/pkg/labels"
	//"k8s.io/metrics/pkg/apis/external_metrics"
)

type dtwScaler struct {
	metadata   *dtwMetadata
	httpClient *http.Client
	logger     logr.Logger
}

type dtwMetadata struct {
	//predictHorizon string
	//historyTimeWindow string
	threshold int64
	host      string
	//queryStep string
	preference string
}

type DtwData struct {
	DtwData float64 `json:"result"`
}

func NewDtwScaler(config *ScalerConfig) (Scaler, error) {

	httpClient := kedautil.CreateHTTPClient(config.GlobalHTTPTimeout, false)

	dtwMetadata, err := parseDtwMetadata(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing dtw metadata: %s", err)
	}

	return &dtwScaler{
		metadata:   dtwMetadata,
		httpClient: httpClient,
		logger:     InitializeLogger(config, "dtw_prediction_scaler"),
	}, nil
}

func parseDtwMetadata(config *ScalerConfig) (*dtwMetadata, error) {

	meta := dtwMetadata{}

	if val, ok := config.TriggerMetadata["threshold"]; ok && val != "" {
		threshold, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("threshold: error parsing threshold %s", err.Error())
		} else {
			meta.threshold = int64(threshold)
		}
	}

	if val, ok := config.TriggerMetadata["host"]; ok {
		_, err := url.ParseRequestURI(val)
		if err != nil {
			return nil, fmt.Errorf("invalid URL: %s", err)
		}
		meta.host = val
	} else {
		return nil, fmt.Errorf("no host URI given")
	}
	if config.TriggerMetadata["preference"] == "" {
		return nil, fmt.Errorf("no preference given")
	}
	meta.preference = config.TriggerMetadata["preference"]

	return &meta, nil
}

func (s *dtwScaler) IsActive(ctx context.Context) (bool, error) {
	prediction, err := s.getDtw()
	if err != nil {
		return false, err
	}

	return (int64(prediction)) > s.metadata.threshold, nil
}

func (s *dtwScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	targetMetricValue := resource.NewQuantity(int64(s.metadata.threshold), resource.DecimalSI)
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: kedautil.NormalizeString(fmt.Sprintf("%s", "dtw-predection")),
		},
		Target: v2.MetricTarget{
			Type:         v2.AverageValueMetricType,
			AverageValue: targetMetricValue,
		},
	}
	metricSpec := v2.MetricSpec{External: externalMetric, Type: externalMetricType}
	return []v2.MetricSpec{metricSpec}
}

func (s *dtwScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {

	prediction, _ := s.getDtw()

	metric := external_metrics.ExternalMetricValue{
		MetricName: metricName,
		Value:      *resource.NewQuantity(int64(prediction), resource.DecimalSI),
		Timestamp:  metav1.Now(),
	}

	return append([]external_metrics.ExternalMetricValue{}, metric), (int64(prediction)) > s.metadata.threshold, nil
}

func (s *dtwScaler) Close(context.Context) error {
	return nil
}

func (s *dtwScaler) getJSONData(out interface{}) error {

	request, err := s.httpClient.Get(s.metadata.host)
	if err != nil {
		return err
	}

	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return err
	}
	err = json.Unmarshal(body, &out)
	if err != nil {
		return err
	}
	return nil
}

func (s *dtwScaler) getDtw() (int, error) {

	var pred int

	var dDat DtwData
	err := s.getJSONData(&dDat)

	if err != nil {
		return 0, fmt.Errorf("getJSONData dDat %s", err.Error())
	}

	switch s.metadata.preference {
	case "Normal":
		pred = int(dDat.DtwData)
	case "MaxP":
		pred = int(dDat.DtwData * 2)
	case "MinP":
		pred = int(12)
	}

	return pred, nil
}
