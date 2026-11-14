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

package e2e

import (
	"context"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turtacn/grovemq/pkg/acl"
	"github.com/turtacn/grovemq/pkg/broker"
)

// TestACLIntegrationDenyTopic verifies, against a real broker, that enabling the
// ACL engine with a topic deny rule blocks publish/subscribe to the denied topic
// (recorded as denials) while non-matching traffic flows normally.
func TestACLIntegrationDenyTopic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokerInstance := broker.New("acl-integration-broker", nil)
	brokerInstance.SetupDefaultAuth()
	defer brokerInstance.Close()

	brokerPort := ":1951"
	go brokerInstance.StartServer(ctx, brokerPort)
	time.Sleep(300 * time.Millisecond)

	engine := brokerInstance.GetACLEngine()
	require.NotNil(t, engine, "ACL engine should be available")
	engine.UpdateConfig(&acl.ACLConfig{Enabled: true, DefaultAction: acl.DefaultActionAllow, LogDenials: false})
	require.NoError(t, engine.GetManager().AddRule(&acl.Rule{
		ID:         "e2e-deny-sensors",
		Permission: acl.PermissionDeny,
		Action:     acl.ActionAll,
		Topic:      "sensors/denied/#",
		Enabled:    true,
		Priority:   1,
	}))

	require.Equal(t, 0, engine.GetStats().DenyCount)

	// Subscribing to the denied topic must be recorded as a denial.
	subClient := createTestMQTTClient("acl-e2e-sub", brokerPort)
	require.True(t, subClient.Connect().WaitTimeout(5*time.Second))
	defer subClient.Disconnect(100)
	subClient.Subscribe("sensors/denied/#", 0, nil).WaitTimeout(2 * time.Second)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 1, engine.GetStats().DenyCount, "denied subscribe must be recorded")

	// Publishing to the denied topic must be recorded as a denial.
	pubClient := createTestMQTTClient("acl-e2e-pub", brokerPort)
	require.True(t, pubClient.Connect().WaitTimeout(5*time.Second))
	defer pubClient.Disconnect(100)
	pubClient.Publish("sensors/denied/temp", 0, false, "nope").WaitTimeout(2 * time.Second)
	time.Sleep(200 * time.Millisecond)
	assert.Equal(t, 2, engine.GetStats().DenyCount, "denied publish must be recorded")

	// Allowed traffic must flow and not add denials.
	msgCh := make(chan mqtt.Message, 1)
	allowSubOpts := mqtt.NewClientOptions()
	allowSubOpts.AddBroker("tcp://localhost" + brokerPort)
	allowSubOpts.SetClientID("acl-e2e-allow-sub")
	allowSubOpts.SetCleanSession(true)
	allowSubOpts.SetConnectTimeout(5 * time.Second)
	allowSubOpts.SetDefaultPublishHandler(func(_ mqtt.Client, msg mqtt.Message) { msgCh <- msg })
	allowSub := mqtt.NewClient(allowSubOpts)
	require.True(t, allowSub.Connect().WaitTimeout(5*time.Second))
	defer allowSub.Disconnect(100)
	require.True(t, allowSub.Subscribe("sensors/allowed/#", 0, nil).WaitTimeout(2*time.Second))

	allowPub := createTestMQTTClient("acl-e2e-allow-pub", brokerPort)
	require.True(t, allowPub.Connect().WaitTimeout(5*time.Second))
	defer allowPub.Disconnect(100)
	require.True(t, allowPub.Publish("sensors/allowed/temp", 0, false, "ok").WaitTimeout(2*time.Second))

	select {
	case m := <-msgCh:
		assert.Equal(t, "sensors/allowed/temp", m.Topic())
		assert.Equal(t, "ok", string(m.Payload()))
	case <-time.After(2 * time.Second):
		t.Fatal("allowed message was not delivered")
	}

	assert.Equal(t, 2, engine.GetStats().DenyCount, "allowed traffic must not add denials")
}
