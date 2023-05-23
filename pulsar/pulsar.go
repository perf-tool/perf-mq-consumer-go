// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/sirupsen/logrus"
	"perf-mq-consumer-go/conf"
)

func Start() error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: fmt.Sprintf("pulsar://%s:%d", conf.PulsarHost, conf.PulsarPort),
	})
	if err != nil {
		return err
	}
	for i := 0; i < conf.RoutineNum; i++ {
		go startConsumer(client)
	}
	return nil
}

func startConsumer(client pulsar.Client) {
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            conf.PulsarTopic,
		SubscriptionName: conf.PulsarSubscriptionName,
		Type:             conf.PulsarSubscriptionType,
	})
	if err != nil {
		logrus.Errorf("create consumer %s error: %v", conf.PulsarTopic, err)
	}
	for {
		message, err := consumer.Receive(context.Background())
		if err != nil {
			logrus.Errorf("receive message %s error: %v", conf.PulsarTopic, err)
		} else {
			logrus.Infof("receive message success, message id: %s, topic: %s",
				message.ID(), message.Topic())
		}
	}
}
