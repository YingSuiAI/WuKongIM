package messageevent

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	benchmetrics "github.com/WuKongIM/WuKongIM/internal/bench/metrics"
)

const (
	// StatusPassed means the run completed without request or metrics errors.
	StatusPassed = "passed"
	// StatusFailed means the run observed at least one request or metrics error.
	StatusFailed = "failed"
)

const (
	groupChannelType = 2
)

// Result is the machine-readable output from one message event stream pressure run.
type Result struct {
	// Status is passed when every public API request and metrics read succeeded.
	Status string `json:"status"`
	// RunID is the stable identifier embedded in generated records.
	RunID string `json:"run_id"`
	// Shape contains the expected request and durable event counts.
	Shape Shape `json:"shape"`
	// StartedAt is the wall-clock start timestamp.
	StartedAt time.Time `json:"started_at"`
	// FinishedAt is the wall-clock finish timestamp.
	FinishedAt time.Time `json:"finished_at"`
	// Duration is the total run duration.
	Duration time.Duration `json:"duration"`
	// Requests summarizes public API requests sent by the runner.
	Requests RequestSummary `json:"requests"`
	// Metrics summarizes before/after Prometheus deltas from the target nodes.
	Metrics benchmetrics.WukongIMAttribution `json:"metrics"`
	// Gates records hard invariants that must hold for a trustworthy stream-cache run.
	Gates []GateResult `json:"gates"`
	// Errors stores bounded human-readable request or metrics errors.
	Errors []string `json:"errors,omitempty"`
	// ReportDir is the directory selected for persisted reports.
	ReportDir string `json:"report_dir"`
}

// GateResult is one hard evidence gate for a message event pressure run.
type GateResult struct {
	// Name is the stable machine-readable gate name.
	Name string `json:"name"`
	// Passed is true when the observed value matches the expected invariant.
	Passed bool `json:"passed"`
	// Expected is the expected metric or request value.
	Expected float64 `json:"expected"`
	// Observed is the measured metric or request value.
	Observed float64 `json:"observed"`
	// Detail is a short human-readable explanation.
	Detail string `json:"detail"`
}

// RequestSummary captures public API request counts and latency percentiles.
type RequestSummary struct {
	// WarmChannelUpserts is the number of /channel setup requests completed before measured metrics.
	WarmChannelUpserts int `json:"warm_channel_upserts"`
	// WarmRuntimeMessages is the number of normal SEND messages completed before measured metrics.
	WarmRuntimeMessages int `json:"warm_runtime_messages"`
	// ChannelUpserts is the number of /channel setup requests that succeeded.
	ChannelUpserts int `json:"channel_upserts"`
	// BaseMessages is the number of /message/send stream base messages that succeeded.
	BaseMessages int `json:"base_messages"`
	// DeltaEvents is the number of delta requests that succeeded.
	DeltaEvents int `json:"delta_events"`
	// FinishEvents is the number of finish requests that succeeded.
	FinishEvents int `json:"finish_events"`
	// Errors is the number of failed public API requests.
	Errors int `json:"errors"`
	// DeltaP50 is the delta request p50 latency.
	DeltaP50 time.Duration `json:"delta_p50"`
	// DeltaP95 is the delta request p95 latency.
	DeltaP95 time.Duration `json:"delta_p95"`
	// DeltaP99 is the delta request p99 latency.
	DeltaP99 time.Duration `json:"delta_p99"`
	// FinishP50 is the finish request p50 latency.
	FinishP50 time.Duration `json:"finish_p50"`
	// FinishP95 is the finish request p95 latency.
	FinishP95 time.Duration `json:"finish_p95"`
	// FinishP99 is the finish request p99 latency.
	FinishP99 time.Duration `json:"finish_p99"`
}

// Runner executes message event stream pressure through public HTTP APIs.
type Runner struct {
	cfg    Config
	client *http.Client
}

// NewRunner constructs a message event stream pressure runner.
func NewRunner(cfg Config) *Runner {
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = DefaultConfig().RequestTimeout
	}
	return &Runner{
		cfg: cfg,
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Run executes the configured pressure run and captures before/after metrics.
func (r *Runner) Run(ctx context.Context) (Result, error) {
	if r == nil {
		return Result{Status: StatusFailed}, fmt.Errorf("messageevent runner is nil")
	}
	cfg := r.cfg
	if cfg.RunID == "" {
		cfg.RunID = DefaultConfig().RunID
	}
	started := time.Now()
	result := Result{
		Status:    StatusFailed,
		RunID:     cfg.RunID,
		Shape:     cfg.Shape(),
		StartedAt: started,
		ReportDir: cfg.ReportDir,
	}
	if err := cfg.Validate(); err != nil {
		result.FinishedAt = time.Now()
		result.Duration = result.FinishedAt.Sub(started)
		result.Errors = append(result.Errors, err.Error())
		return result, err
	}

	recorder := &requestRecorder{}
	if cfg.WarmChannels {
		if err := r.setupChannels(ctx, cfg, recorder, true); err != nil {
			recorder.recordError(err)
			result.Requests = recorder.summary()
			result.FinishedAt = time.Now()
			result.Duration = result.FinishedAt.Sub(started)
			result.Errors = recorder.errorSamples()
			return result, err
		}
	}
	if cfg.WarmRuntime {
		if err := r.warmChannelRuntime(ctx, cfg, recorder); err != nil {
			recorder.recordError(err)
			result.Requests = recorder.summary()
			result.FinishedAt = time.Now()
			result.Duration = result.FinishedAt.Sub(started)
			result.Errors = recorder.errorSamples()
			return result, err
		}
	}

	before, err := r.readMetrics(ctx, cfg)
	if err != nil {
		result.FinishedAt = time.Now()
		result.Duration = result.FinishedAt.Sub(started)
		result.Requests = recorder.summary()
		result.Errors = append(result.Errors, fmt.Sprintf("read before metrics: %v", err))
		return result, err
	}

	runErr := r.runTraffic(ctx, cfg, recorder)
	after, metricsErr := r.readMetrics(ctx, cfg)
	if metricsErr != nil {
		recorder.recordError(fmt.Errorf("read after metrics: %w", metricsErr))
	}
	result.Metrics = benchmetrics.AnalyzeWukongIMPrometheus(before, after)
	result.Requests = recorder.summary()
	result.Gates = evaluateGates(result)
	result.Errors = recorder.errorSamples()
	result.FinishedAt = time.Now()
	result.Duration = result.FinishedAt.Sub(started)
	gatesOK := gatesPassed(result.Gates)
	if runErr == nil && metricsErr == nil && result.Requests.Errors == 0 && gatesOK {
		result.Status = StatusPassed
		return result, nil
	}
	if runErr != nil {
		return result, runErr
	}
	if metricsErr != nil {
		return result, metricsErr
	}
	if !gatesOK {
		return result, fmt.Errorf("message event hard gates failed")
	}
	return result, nil
}

func (r *Runner) runTraffic(ctx context.Context, cfg Config, recorder *requestRecorder) error {
	if !cfg.WarmChannels {
		if err := r.setupChannels(ctx, cfg, recorder, false); err != nil {
			return err
		}
	}

	streams := make(chan streamSpec)
	workers := cfg.Concurrency
	if workers > cfg.Shape().Streams {
		workers = cfg.Shape().Streams
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for spec := range streams {
				if err := r.runStream(ctx, cfg, spec, recorder); err != nil {
					recorder.recordError(err)
				}
			}
		}()
	}
	for channelIndex := 0; channelIndex < cfg.Channels; channelIndex++ {
		for streamIndex := 0; streamIndex < cfg.StreamsPerChannel; streamIndex++ {
			select {
			case <-ctx.Done():
				close(streams)
				wg.Wait()
				return ctx.Err()
			case streams <- streamSpec{channelIndex: channelIndex, streamIndex: streamIndex}:
			}
		}
	}
	close(streams)
	wg.Wait()
	if recorder.errors() > 0 {
		return fmt.Errorf("%d message event stream requests failed", recorder.errors())
	}
	return nil
}

func (r *Runner) setupChannels(ctx context.Context, cfg Config, recorder *requestRecorder, warm bool) error {
	for i := 0; i < cfg.Channels; i++ {
		if err := r.setupChannel(ctx, cfg, i); err != nil {
			recorder.recordError(err)
			return err
		}
		recorder.recordChannelUpsert(warm)
	}
	return nil
}

func (r *Runner) warmChannelRuntime(ctx context.Context, cfg Config, recorder *requestRecorder) error {
	for i := 0; i < cfg.Channels; i++ {
		if err := r.sendWarmRuntimeMessage(ctx, cfg, i); err != nil {
			return err
		}
		recorder.recordWarmRuntimeMessage()
	}
	return nil
}

func (r *Runner) sendWarmRuntimeMessage(ctx context.Context, cfg Config, index int) error {
	api := cfg.APIAddrs[index%len(cfg.APIAddrs)]
	if err := r.postJSON(ctx, api, "/message/send", map[string]any{
		"from_uid":      generatedUID(cfg, index),
		"channel_id":    generatedChannelID(cfg, index),
		"channel_type":  groupChannelType,
		"client_msg_no": generatedWarmRuntimeClientMsgNo(cfg, index),
		"setting":       0,
		"payload":       base64.StdEncoding.EncodeToString([]byte("warm runtime")),
	}, nil); err != nil {
		return fmt.Errorf("warm channel runtime %s: %w", generatedChannelID(cfg, index), err)
	}
	return nil
}

type streamSpec struct {
	channelIndex int
	streamIndex  int
}

func (r *Runner) runStream(ctx context.Context, cfg Config, spec streamSpec, recorder *requestRecorder) error {
	channelID := generatedChannelID(cfg, spec.channelIndex)
	fromUID := generatedUID(cfg, spec.channelIndex)
	clientMsgNo := generatedClientMsgNo(cfg, spec.channelIndex, spec.streamIndex)
	runID := generatedRunID(spec.channelIndex, spec.streamIndex)
	authorizationFence := uint64(1)
	api := cfg.APIAddrs[(spec.channelIndex+spec.streamIndex)%len(cfg.APIAddrs)]
	var sendResult struct {
		MessageID  uint64 `json:"message_id"`
		MessageSeq uint64 `json:"message_seq"`
	}
	if err := r.postJSON(ctx, api, "/message/send", map[string]any{
		"from_uid":      fromUID,
		"channel_id":    channelID,
		"channel_type":  groupChannelType,
		"client_msg_no": clientMsgNo,
		"payload":       base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf(`{"type":1,"run_id":%q,"authorization_fence":%d}`, runID, authorizationFence))),
	}, &sendResult); err != nil {
		return fmt.Errorf("send base stream %s: %w", clientMsgNo, err)
	}
	if sendResult.MessageID == 0 {
		return fmt.Errorf("send base stream %s: response omitted message_id", clientMsgNo)
	}
	recorder.recordBaseMessage()

	authoritySequence := uint64(0)
	for lane := 0; lane < cfg.LanesPerStream; lane++ {
		for delta := 0; delta < cfg.DeltasPerLane; delta++ {
			authoritySequence++
			eventID := generatedEventID(cfg, spec.channelIndex, spec.streamIndex, lane, delta)
			eventKey := generatedEventKey(lane)
			occurredAt := time.Now().UTC()
			textDelta := generatedPayload(cfg.PayloadBytes)
			start := time.Now()
			if err := r.postJSON(ctx, api, "/message/events:append", map[string]any{
				"channel_id":          channelID,
				"channel_type":        groupChannelType,
				"from_uid":            fromUID,
				"message_id":          sendResult.MessageID,
				"client_msg_no":       clientMsgNo,
				"run_id":              runID,
				"authorization_fence": authorizationFence,
				"authority_sequence":  authoritySequence,
				"event_id":            eventID,
				"event_key":           eventKey,
				"event_type":          "delta",
				"occurred_at":         occurredAt.UnixMilli(),
				"payload":             generatedAgentRunDeltaPayload(eventID, runID, channelID, clientMsgNo, fromUID, sendResult.MessageSeq, eventKey, authoritySequence, authorizationFence, occurredAt, textDelta),
			}, nil); err != nil {
				return fmt.Errorf("append delta %s lane %d delta %d: %w", clientMsgNo, lane, delta, err)
			}
			recorder.recordDelta(time.Since(start))
		}
	}

	start := time.Now()
	authoritySequence++
	finishEventID := generatedFinishEventID(spec.channelIndex, spec.streamIndex)
	finishOccurredAt := time.Now().UTC()
	if err := r.postJSON(ctx, api, "/message/events:append", map[string]any{
		"channel_id":          channelID,
		"channel_type":        groupChannelType,
		"from_uid":            fromUID,
		"message_id":          sendResult.MessageID,
		"client_msg_no":       clientMsgNo,
		"run_id":              runID,
		"authorization_fence": authorizationFence,
		"authority_sequence":  authoritySequence,
		"event_id":            finishEventID,
		"event_key":           "main",
		"event_type":          "finish",
		"occurred_at":         finishOccurredAt.UnixMilli(),
		"payload":             generatedAgentRunFinishPayload(finishEventID, runID, channelID, clientMsgNo, fromUID, sendResult.MessageSeq, authoritySequence, authorizationFence, finishOccurredAt),
	}, nil); err != nil {
		return fmt.Errorf("finish stream %s: %w", clientMsgNo, err)
	}
	recorder.recordFinish(time.Since(start))
	return nil
}

func (r *Runner) setupChannel(ctx context.Context, cfg Config, index int) error {
	api := cfg.APIAddrs[index%len(cfg.APIAddrs)]
	return r.postJSON(ctx, api, "/channel", map[string]any{
		"channel_id":   generatedChannelID(cfg, index),
		"channel_type": groupChannelType,
		"reset":        1,
		"subscribers":  []string{generatedUID(cfg, index)},
	}, nil)
}

func (r *Runner) readMetrics(ctx context.Context, cfg Config) (benchmetrics.PrometheusSnapshot, error) {
	var out benchmetrics.PrometheusSnapshot
	for _, api := range cfg.APIAddrs {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, joinURL(api, "/metrics"), nil)
		if err != nil {
			return benchmetrics.PrometheusSnapshot{}, err
		}
		resp, err := r.client.Do(req)
		if err != nil {
			return benchmetrics.PrometheusSnapshot{}, err
		}
		body, readErr := io.ReadAll(resp.Body)
		closeErr := resp.Body.Close()
		if readErr != nil {
			return benchmetrics.PrometheusSnapshot{}, readErr
		}
		if closeErr != nil {
			return benchmetrics.PrometheusSnapshot{}, closeErr
		}
		if resp.StatusCode/100 != 2 {
			return benchmetrics.PrometheusSnapshot{}, fmt.Errorf("GET %s returned %d: %s", joinURL(api, "/metrics"), resp.StatusCode, strings.TrimSpace(string(body)))
		}
		snap, err := benchmetrics.ParsePrometheusText(bytes.NewReader(body))
		if err != nil {
			return benchmetrics.PrometheusSnapshot{}, fmt.Errorf("parse metrics from %s: %w", api, err)
		}
		out.Samples = append(out.Samples, snap.Samples...)
	}
	return out, nil
}

func (r *Runner) postJSON(ctx context.Context, api string, path string, body any, out any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, joinURL(api, path), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if path == "/message/events:append" {
		req.Header.Set("Authorization", "Bearer "+r.cfg.ServiceToken)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("POST %s returned %d: %s", joinURL(api, path), resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	var status struct {
		Status *int   `json:"status"`
		Msg    string `json:"msg"`
		Error  string `json:"error"`
	}
	if err := json.Unmarshal(respBody, &status); err == nil && status.Status != nil && *status.Status != http.StatusOK {
		message := strings.TrimSpace(status.Msg)
		if message == "" {
			message = strings.TrimSpace(status.Error)
		}
		if message == "" {
			message = http.StatusText(*status.Status)
		}
		return fmt.Errorf("POST %s returned business status %d: %s", joinURL(api, path), *status.Status, message)
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decode POST %s: %w", joinURL(api, path), err)
		}
	}
	return nil
}

func joinURL(base string, path string) string {
	return strings.TrimRight(base, "/") + path
}

func generatedChannelID(cfg Config, index int) string {
	return fmt.Sprintf("wkbench-message-event-%s-ch-%06d", sanitizeRunID(cfg.RunID), index)
}

func generatedUID(cfg Config, index int) string {
	return fmt.Sprintf("wkbench-message-event-%s-u-%06d", sanitizeRunID(cfg.RunID), index)
}

func generatedClientMsgNo(cfg Config, channelIndex int, streamIndex int) string {
	return fmt.Sprintf("wkbench-message-event-%s-c%06d-s%06d", sanitizeRunID(cfg.RunID), channelIndex, streamIndex)
}

func generatedWarmRuntimeClientMsgNo(cfg Config, channelIndex int) string {
	return fmt.Sprintf("wkbench-message-event-%s-c%06d-warm-runtime-000000", sanitizeRunID(cfg.RunID), channelIndex)
}

func generatedEventID(cfg Config, channelIndex int, streamIndex int, lane int, delta int) string {
	return fmt.Sprintf("019d%04x-%04x-7000-8000-%012x", channelIndex&0xffff, streamIndex&0xffff, uint64(lane&0xff)<<32|uint64(delta&0xffffffff))
}

func generatedRunID(channelIndex int, streamIndex int) string {
	return fmt.Sprintf("019d%04x-%04x-7000-8001-%012x", channelIndex&0xffff, streamIndex&0xffff, uint64(streamIndex+1))
}

func generatedFinishEventID(channelIndex int, streamIndex int) string {
	return fmt.Sprintf("019d%04x-%04x-7000-8002-%012x", channelIndex&0xffff, streamIndex&0xffff, uint64(streamIndex+1))
}

func generatedAgentRunDeltaPayload(eventID, runID, conversationID, clientMsgNo, fromUID string, messageSequence uint64, eventKey string, authoritySequence, authorizationFence uint64, occurredAt time.Time, textDelta string) map[string]any {
	digest := strings.Repeat("a", 64)
	return generatedAgentRunPayload(eventID, runID, conversationID, clientMsgNo, fromUID, messageSequence, eventKey, "delta", authoritySequence, authorizationFence, occurredAt, digest, map[string]any{"text_delta": textDelta})
}

func generatedAgentRunFinishPayload(eventID, runID, conversationID, clientMsgNo, fromUID string, messageSequence, authoritySequence, authorizationFence uint64, occurredAt time.Time) map[string]any {
	digest := strings.Repeat("b", 64)
	return generatedAgentRunPayload(eventID, runID, conversationID, clientMsgNo, fromUID, messageSequence, "main", "finish", authoritySequence, authorizationFence, occurredAt, digest, map[string]any{"snapshot": map[string]any{
		"state": "succeeded", "authority_sequence": authoritySequence, "text": "", "complete": true, "projection_digest_sha256": digest,
	}})
}

func generatedAgentRunPayload(eventID, runID, conversationID, clientMsgNo, fromUID string, messageSequence uint64, eventKey, eventType string, authoritySequence, authorizationFence uint64, occurredAt time.Time, digest string, variant map[string]any) map[string]any {
	payload := map[string]any{
		"event_id": eventID, "run_id": runID,
		"base_message": map[string]any{
			"conversation_id": generatedContractUUID(conversationID, 1), "message_id": generatedContractUUID(clientMsgNo, 2),
			"client_msg_id": clientMsgNo, "committed_message_ref": "agent-run." + runID,
			"message_sequence": messageSequence, "source_principal_id": generatedContractUUID(fromUID, 3),
		},
		"event_key": eventKey, "event_type": eventType, "authority_sequence": authoritySequence,
		"projection_digest_sha256": digest, "authorization_fence": authorizationFence, "occurred_at": occurredAt.Format(time.RFC3339Nano),
	}
	for key, value := range variant {
		payload[key] = value
	}
	return payload
}

func generatedContractUUID(value string, domain byte) string {
	_ = value
	return fmt.Sprintf("00000000-0000-7000-8000-%012x", domain)
}

func generatedEventKey(lane int) string {
	if lane == 0 {
		return "main"
	}
	return fmt.Sprintf("lane-%03d", lane)
}

func generatedPayload(size int) string {
	if size <= 0 {
		return ""
	}
	return strings.Repeat("x", size)
}

func sanitizeRunID(runID string) string {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return "message-event-stream"
	}
	var b strings.Builder
	for _, r := range runID {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	return b.String()
}

type requestRecorder struct {
	mu                  sync.Mutex
	warmChannelUpserts  int
	warmRuntimeMessages int
	channelUpserts      int
	baseMessages        int
	deltaEvents         int
	finishEvents        int
	errs                int
	samples             []string
	deltaDurations      []time.Duration
	finishDurations     []time.Duration
}

func (r *requestRecorder) recordChannelUpsert(warm bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if warm {
		r.warmChannelUpserts++
		return
	}
	r.channelUpserts++
}

func (r *requestRecorder) recordWarmRuntimeMessage() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.warmRuntimeMessages++
}

func (r *requestRecorder) recordBaseMessage() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.baseMessages++
}

func (r *requestRecorder) recordDelta(dur time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deltaEvents++
	r.deltaDurations = append(r.deltaDurations, dur)
}

func (r *requestRecorder) recordFinish(dur time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.finishEvents++
	r.finishDurations = append(r.finishDurations, dur)
}

func (r *requestRecorder) recordError(err error) {
	if err == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs++
	if len(r.samples) < 16 {
		r.samples = append(r.samples, err.Error())
	}
}

func (r *requestRecorder) errors() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.errs
}

func (r *requestRecorder) errorSamples() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.samples...)
}

func (r *requestRecorder) summary() RequestSummary {
	r.mu.Lock()
	defer r.mu.Unlock()
	return RequestSummary{
		WarmChannelUpserts:  r.warmChannelUpserts,
		WarmRuntimeMessages: r.warmRuntimeMessages,
		ChannelUpserts:      r.channelUpserts,
		BaseMessages:        r.baseMessages,
		DeltaEvents:         r.deltaEvents,
		FinishEvents:        r.finishEvents,
		Errors:              r.errs,
		DeltaP50:            percentileDuration(r.deltaDurations, 0.50),
		DeltaP95:            percentileDuration(r.deltaDurations, 0.95),
		DeltaP99:            percentileDuration(r.deltaDurations, 0.99),
		FinishP50:           percentileDuration(r.finishDurations, 0.50),
		FinishP95:           percentileDuration(r.finishDurations, 0.95),
		FinishP99:           percentileDuration(r.finishDurations, 0.99),
	}
}

func evaluateGates(result Result) []GateResult {
	shape := result.Shape
	metrics := result.Metrics
	return []GateResult{
		newGate("request_errors_zero", 0, float64(result.Requests.Errors), "public API request errors must stay at zero"),
		newGate("delta_cache_count_matches", float64(shape.DeltaEvents), metrics.MessageEventAppendCountByPath["cache"], "delta requests must stay cache-only"),
		newBoundedGate("finish_propose_count_bounded", 1, float64(shape.ExpectedFinishProposals), metrics.MessageEventProposeCountByPath["finish_batch"], "finish proposals may coalesce but must stay non-zero and at or below finish count"),
		newGate("cache_miss_zero", 0, metrics.MessageEventCacheMissCount, "stream cache misses must stay at zero"),
		newGate("backpressured_zero", 0, metrics.MessageEventBackpressuredCount, "message event backpressure must stay at zero"),
	}
}

func newGate(name string, expected float64, observed float64, detail string) GateResult {
	return GateResult{
		Name:     name,
		Passed:   expected == observed,
		Expected: expected,
		Observed: observed,
		Detail:   detail,
	}
}

func newBoundedGate(name string, minimum float64, maximum float64, observed float64, detail string) GateResult {
	return GateResult{
		Name:     name,
		Passed:   observed >= minimum && observed <= maximum,
		Expected: maximum,
		Observed: observed,
		Detail:   detail,
	}
}

func gatesPassed(gates []GateResult) bool {
	for _, gate := range gates {
		if !gate.Passed {
			return false
		}
	}
	return true
}

func percentileDuration(values []time.Duration, q float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	cp := append([]time.Duration(nil), values...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	rank := int(math.Ceil(q*float64(len(cp)))) - 1
	if rank < 0 {
		rank = 0
	}
	if rank >= len(cp) {
		rank = len(cp) - 1
	}
	return cp[rank]
}
