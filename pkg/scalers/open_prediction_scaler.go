package scalers

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	"github.com/go-playground/validator/v10"
	"github.com/xhit/go-str2duration/v2"
	v2 "k8s.io/api/autoscaling/v2"
	"k8s.io/metrics/pkg/apis/external_metrics"

	kedautil "github.com/kedacore/keda/v2/pkg/util"

	"github.com/mhdbashar/prediction-go/prediction"
	"google.golang.org/grpc"

	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const (
	openPredictionMetricType   = "External"
	openPredictionMetricPrefix = "openPrediction_metric"
)

var (
	default_step = "15m"
)

type openPredictionScaler struct {
	metrictype       v2.MetricTargetType
	metadata         *openPredictionMetadata
	prometheusClient api.Client
	api              v1.API
	grpcConn         *grpc.ClientConn
	grpcClient       prediction.PredictionServiceClient
	logger           logr.Logger
}

type openPredictionMetadata struct {
	history                  string
	stepDuration             string
	verticalWindow           int
	horizontalWindow         int
	prometheusAddress        string
	predictionServiceAddress string
	query                    string
	threshold                float64
	activationThreshold      float64
	scalerIndex              int
}

func (s *openPredictionScaler) setupClientConn(predictionServiceAddress string) error {

	conn, err := grpc.Dial(predictionServiceAddress, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
		return err
	}
	s.grpcConn = conn

	client := prediction.NewPredictionServiceClient(conn)
	s.grpcClient = client

	return err
}

func NewopenPredictionScaler(ctx context.Context, config *ScalerConfig) (Scaler, error) {
	// logger metrictype metadata prometheusClie api grpcConn  grpcClient

	s := &openPredictionScaler{}

	// logger
	logger := InitializeLogger(config, "openPrediction_scaler")
	s.logger = logger

	// metrictype
	metrictype, err := GetMetricTargetType(config)
	if err != nil {
		logger.Error(err, "error getting scaler metric type")
		return nil, fmt.Errorf("error getting scaler metric type: %w", err)
	}
	s.metrictype = metrictype

	// metadata
	meta, err := parseOpenPredictionMetaData(config)
	if err != nil {
		return nil, fmt.Errorf("error parsing OpenPrediction metadata: %s", err)
	}
	s.metadata = meta

	// prometheusClient api
	err = s.initOpenPredictionPrometheusConn(ctx)
	if err != nil {
		logger.Error(err, "error create Prometheus client and API objects")
		return nil, fmt.Errorf("error create Prometheus client and API objects: %3s", err)
	}

	// grpcConn grpcClient
	err = s.setupClientConn(meta.predictionServiceAddress)
	if err != nil {
		logger.Error(err, "error init GRPC client")
		return nil, fmt.Errorf("error init GRPC client: %3s", err)
	}

	return s, nil
}

func parseOpenPredictionMetaData(config *ScalerConfig) (result *openPredictionMetadata, err error) {

	validate := validator.New()
	meta := openPredictionMetadata{}

	//history
	if val, ok := config.TriggerMetadata["history"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no history given")
		}
		meta.history = val
	} else {
		return nil, fmt.Errorf("no history given")
	}

	//stepDuration
	if val, ok := config.TriggerMetadata["stepDuration"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no stepDuration given")
		}
		meta.stepDuration = val
	} else {
		return nil, fmt.Errorf("no stepDuration given")
	}

	//verticalWindow
	if val, ok := config.TriggerMetadata["verticalWindow"]; ok {
		verticalWindow, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("verticalWindow parsing error %w", err)
		}
		meta.verticalWindow = verticalWindow
	} else {
		return nil, fmt.Errorf("no verticalWindow given")
	}

	//horizontalWindow
	if val, ok := config.TriggerMetadata["horizontalWindow"]; ok {
		horizontalWindow, err := strconv.Atoi(val)
		if err != nil {
			return nil, fmt.Errorf("horizontalWindow parsing error %w", err)
		}
		meta.horizontalWindow = horizontalWindow
	} else {
		return nil, fmt.Errorf("no horizontalWindow given")
	}

	//prometheusAddress
	if val, ok := config.TriggerMetadata["prometheusAddress"]; ok {
		err = validate.Var(val, "url")
		if err != nil {
			return nil, fmt.Errorf("invalid prometheusAddress")
		}
		meta.prometheusAddress = val
	} else {
		return nil, fmt.Errorf("no prometheusAddress given")
	}

	// predictionServiceAddress
	if val, ok := config.TriggerMetadata["predictionServiceAddress"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("invalid predictionServiceAddress")
		}
		meta.predictionServiceAddress = val
	} else {
		return nil, fmt.Errorf("no predictionServiceAddress given")
	}

	//query
	if val, ok := config.TriggerMetadata["query"]; ok {
		if len(val) == 0 {
			return nil, fmt.Errorf("no query given")
		}
		meta.query = val
	} else {
		return nil, fmt.Errorf("no query given")
	}

	//threshold
	if val, ok := config.TriggerMetadata["threshold"]; ok {
		threshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("threshold parsing error %w", err)
		}
		meta.threshold = threshold
	} else {
		return nil, fmt.Errorf("no threshold given")
	}

	//activationThreshold
	meta.activationThreshold = 0
	if val, ok := config.TriggerMetadata["activationThreshold"]; ok {
		activationThreshold, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return nil, fmt.Errorf("activationThreshold parsing error %w", err)
		}
		meta.activationThreshold = activationThreshold
	}

	// scalerIndex
	meta.scalerIndex = config.ScalerIndex

	return &meta, nil
}

func (s *openPredictionScaler) Close(_ context.Context) error {
	if s != nil && s.grpcConn != nil {
		return s.grpcConn.Close()
	}
	return nil
}

func (s *openPredictionScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	metricName := kedautil.NormalizeString(fmt.Sprintf("openPrediction-%s", openPredictionMetricPrefix))
	externalMetric := &v2.ExternalMetricSource{
		Metric: v2.MetricIdentifier{
			Name: GenerateMetricNameWithIndex(s.metadata.scalerIndex, metricName),
		},
		Target: GetMetricTargetMili(s.metrictype, s.metadata.threshold),
	}

	metricSpec := v2.MetricSpec{
		External: externalMetric, Type: openPredictionMetricType,
	}
	return []v2.MetricSpec{metricSpec}
}

func (s *openPredictionScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	value, activationValue, err := s.doPredictRequest(ctx)
	if err != nil {
		s.logger.Error(err, "error executing query to predict controller service")
		return []external_metrics.ExternalMetricValue{}, false, err
	}

	if value == 0 {
		s.logger.V(1).Info("empty response after predict request")
		return []external_metrics.ExternalMetricValue{}, false, nil
	}

	s.logger.V(1).Info(fmt.Sprintf("predict value is: %f", value))

	metric := GenerateMetricInMili(metricName, value)

	return []external_metrics.ExternalMetricValue{metric}, activationValue > s.metadata.activationThreshold, nil
}

func (s *openPredictionScaler) doPredictRequest(ctx context.Context) (float64, float64, error) {
	results, err := s.doQuery(ctx)
	if err != nil {
		return 0, 0, err
	}

	metricName := GenerateMetricNameWithIndex(s.metadata.scalerIndex, kedautil.NormalizeString(fmt.Sprintf("openPrediction-%s", openPredictionMetricPrefix)))

	requestData := &prediction.PredictionRequest{
		MicorserviceName:        metricName,
		Measurements:            results,
		History:                 s.metadata.history,
		StepDuration:            s.metadata.stepDuration,
		PredictVerticalWindow:   int32(s.metadata.verticalWindow),
		PredictHorizontalWindow: int32(s.metadata.horizontalWindow),
	}

	resp, err := s.grpcClient.ProcessData(ctx, requestData)

	if err != nil {
		return 0, 0, err
	}

	var y float64
	if len(results) > 0 {
		y = results[len(results)-1]
	}

	x := float64(resp.Result)

	return func(x, y float64) float64 {
		if x < y {
			return y
		}
		return x
	}(x, y), y, nil
}

func (s *openPredictionScaler) doQuery(ctx context.Context) ([]float64, error) {
	currentTime := time.Now().UTC()

	if s.metadata.stepDuration == "" {
		s.metadata.stepDuration = default_step
	}

	//parse string to Duration
	history, err := str2duration.ParseDuration(s.metadata.history)
	if err != nil {
		return nil, fmt.Errorf("history parsing error %w", err)
	}

	stepDuration, err := str2duration.ParseDuration(s.metadata.stepDuration)
	if err != nil {
		return nil, fmt.Errorf("stepDuration parsing error %w", err)
	}

	r := v1.Range{
		Start: currentTime.Add(-history),
		End:   currentTime,
		Step:  stepDuration,
	}

	val, warns, err := s.api.QueryRange(ctx, s.metadata.query, r)

	if len(warns) > 0 {
		s.logger.V(1).Info("warnings", warns)
	}

	if err != nil {
		return nil, err
	}

	return s.parsePrometheusResult(val)
}

// parsePrometheusResult parsing response from prometheus server.
func (s *openPredictionScaler) parsePrometheusResult(result model.Value) (out []float64, err error) {
	switch result.Type() {
	case model.ValVector:
		if res, ok := result.(model.Vector); ok {
			for _, val := range res {
				if err != nil {
					return nil, err
				}
				out = append(out, float64(val.Value))
			}
		}
	case model.ValMatrix:
		if res, ok := result.(model.Matrix); ok {
			for _, val := range res {
				for _, v := range val.Values {
					if err != nil {
						return nil, err
					}

					out = append(out, float64(v.Value))
				}
			}
		}
	case model.ValScalar:
		if res, ok := result.(*model.Scalar); ok {
			if err != nil {
				return nil, err
			}

			out = append(out, float64(res.Value))
		}
	case model.ValString:
		if res, ok := result.(*model.String); ok {
			if err != nil {
				return nil, err
			}

			s, err := strconv.ParseFloat(res.Value, 64)
			if err != nil {
				return nil, err
			}

			out = append(out, s)
		}
	default:
		return nil, err
	}

	return out, nil
}

// initOpenPredictionPrometheusConn init prometheus client and setup connection to API
func (s *openPredictionScaler) initOpenPredictionPrometheusConn(ctx context.Context) (err error) {

	client, err := api.NewClient(api.Config{
		Address: s.metadata.prometheusAddress,
	})
	if err != nil {
		s.logger.V(1).Error(err, "init Prometheus client")
		return err
	}
	s.prometheusClient = client
	s.api = v1.NewAPI(s.prometheusClient)

	return s.ping(ctx)
}

func (s *openPredictionScaler) ping(ctx context.Context) (err error) {
	_, err = s.api.Runtimeinfo(ctx)
	return err
}
