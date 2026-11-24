// Copyright 2023 The grovemq Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package a2a defines the shared Prometheus metrics for GroveMQ's A2A
// Registry, task state machine and HTTP gateway.
package a2a

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// RegistryAgents reports the current number of registered agents by status.
	RegistryAgents = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "a2a_registry_agents",
		Help: "Number of agents currently in the A2A Registry.",
	}, []string{"status"})

	// RegistrationsTotal counts card registrations, unregistrations and
	// rejected publishes.
	RegistrationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "a2a_registrations_total",
		Help: "Total A2A agent card registration operations.",
	}, []string{"result"})

	// CardValidationFailures counts Agent Cards rejected by schema validation.
	CardValidationFailures = promauto.NewCounter(prometheus.CounterOpts{
		Name: "a2a_card_validation_failures",
		Help: "Total A2A Agent Cards rejected by schema validation.",
	})

	// StatusChangesTotal counts online/offline transitions and their source.
	StatusChangesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "a2a_status_changes",
		Help: "Total A2A agent status changes.",
	}, []string{"status", "source"})

	// TasksTotal counts task state transitions.
	TasksTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "a2a_tasks_total",
		Help: "Total A2A task state transitions.",
	}, []string{"state"})

	// TaskDurationSeconds observes time spent in a task before it reaches a
	// terminal state. SubmittedAt is supplied by the caller.
	TaskDurationSeconds = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "a2a_task_duration_seconds",
		Help:    "Duration of A2A tasks by terminal state.",
		Buckets: prometheus.DefBuckets,
	}, []string{"state"})

	// GatewayRequestsTotal counts HTTP JSON-RPC gateway calls.
	GatewayRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "a2a_gateway_requests_total",
		Help: "Total HTTP JSON-RPC A2A gateway requests.",
	}, []string{"method", "result"})
)

// SetRegistryAgents updates the registry gauges from a stats snapshot.
func SetRegistryAgents(registered, online, offline int) {
	RegistryAgents.WithLabelValues("online").Set(float64(online))
	RegistryAgents.WithLabelValues("offline").Set(float64(offline))
	RegistryAgents.WithLabelValues("total").Set(float64(registered))
}

// IncRegistration records a registration operation outcome.
func IncRegistration(result string) {
	RegistrationsTotal.WithLabelValues(result).Inc()
}

// IncStatusChange records a status transition.
func IncStatusChange(status, source string) {
	StatusChangesTotal.WithLabelValues(status, source).Inc()
}

// ObserveTaskState records a task state transition.
func ObserveTaskState(state string) {
	TasksTotal.WithLabelValues(state).Inc()
}

// ObserveTaskDuration records the duration of a completed task.
func ObserveTaskDuration(state string, d time.Duration) {
	if d < 0 {
		d = 0
	}
	TaskDurationSeconds.WithLabelValues(state).Observe(d.Seconds())
}

// IncGatewayRequest records a gateway call outcome.
func IncGatewayRequest(method, result string) {
	GatewayRequestsTotal.WithLabelValues(method, result).Inc()
}
