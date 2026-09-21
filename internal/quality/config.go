package quality

import (
	"fmt"
	"math"
	"strings"
	"time"
)

type Config struct {
	LatencyAlpha                float64
	ErrorAlpha                  float64
	MinimumSampleCount          uint64
	ConsecutiveFailureThreshold int32
	ErrorRateThreshold          float64
	LatencyDegradedFactor       float64
	MetricStaleAfter            time.Duration
	BaseEjectionTime            time.Duration
	MaxEjectionTime             time.Duration
	MaxEjectedPercent           int32
	HealthySamplesToRecover     int32
	MinimumWeightDelta          int32
	RecoverySteps               []time.Duration
	HardStaleAfter              time.Duration
}

func LabDefaults() Config {
	return Config{
		LatencyAlpha:                0.2,
		ErrorAlpha:                  0.3,
		MinimumSampleCount:          50,
		ConsecutiveFailureThreshold: 5,
		ErrorRateThreshold:          0.10,
		LatencyDegradedFactor:       12,
		MetricStaleAfter:            30 * time.Second,
		HardStaleAfter:              5 * time.Minute,
		BaseEjectionTime:            30 * time.Second,
		MaxEjectionTime:             5 * time.Minute,
		MaxEjectedPercent:           50,
		HealthySamplesToRecover:     3,
		MinimumWeightDelta:          5,
		RecoverySteps:               []time.Duration{30 * time.Second, 60 * time.Second, 120 * time.Second},
	}
}

func ParseRecoverySteps(value string) ([]time.Duration, error) {
	parts := strings.Split(value, ",")
	steps := make([]time.Duration, len(parts))
	for i, part := range parts {
		duration, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("parse recovery step %q: %w", part, err)
		}
		steps[i] = duration
	}
	return steps, nil
}

func (c Config) Validate() error {
	if c.LatencyAlpha < 0 || c.LatencyAlpha > 1 || math.IsNaN(c.LatencyAlpha) {
		return fmt.Errorf("latency EWMA alpha must be within [0,1]")
	}
	if c.ErrorAlpha < 0 || c.ErrorAlpha > 1 || math.IsNaN(c.ErrorAlpha) {
		return fmt.Errorf("error EWMA alpha must be within [0,1]")
	}
	if c.MinimumSampleCount == 0 || c.ConsecutiveFailureThreshold <= 0 {
		return fmt.Errorf("sample and consecutive failure thresholds must be positive")
	}
	if c.ErrorRateThreshold < 0 || c.ErrorRateThreshold > 1 || math.IsNaN(c.ErrorRateThreshold) {
		return fmt.Errorf("error rate threshold must be within [0,1]")
	}
	if c.LatencyDegradedFactor < 1 || math.IsNaN(c.LatencyDegradedFactor) || math.IsInf(c.LatencyDegradedFactor, 0) {
		return fmt.Errorf("latency degraded factor must be at least 1")
	}
	if c.MetricStaleAfter <= 0 || c.HardStaleAfter <= c.MetricStaleAfter {
		return fmt.Errorf("hard stale duration must exceed metric stale duration")
	}
	if c.BaseEjectionTime <= 0 || c.MaxEjectionTime < c.BaseEjectionTime {
		return fmt.Errorf("maximum ejection time must be at least the base ejection time")
	}
	if c.MaxEjectedPercent < 1 || c.MaxEjectedPercent > 100 {
		return fmt.Errorf("maximum ejected percent must be within [1,100]")
	}
	if c.HealthySamplesToRecover <= 0 || c.MinimumWeightDelta < 0 || c.MinimumWeightDelta > 100 {
		return fmt.Errorf("recovery samples and weight delta are outside valid bounds")
	}
	if len(c.RecoverySteps) != 3 || c.RecoverySteps[0] <= 0 || c.RecoverySteps[1] <= c.RecoverySteps[0] || c.RecoverySteps[2] <= c.RecoverySteps[1] {
		return fmt.Errorf("recovery steps must contain three increasing positive durations")
	}
	return nil
}
