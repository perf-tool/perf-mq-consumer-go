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

package conf

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"os"
	"perf-mq-consumer-go/util"
)

const (
	Exclusive = "Exclusive"

	Shared = "Shared"

	Failover = "Failover"

	KeyShared = "KeyShared"
)

var SubTypeEnum = map[string]pulsar.SubscriptionType{
	"Exclusive": pulsar.Exclusive,

	"Shared": pulsar.Shared,

	"Failover": pulsar.Failover,

	"KeyShared": pulsar.KeyShared,
}

// pulsar server environment config
var (
	PulsarHost             = util.GetEnvStr("PULSAR_HOST", "localhost")
	PulsarPort             = util.GetEnvInt("PULSAR_PORT", 6650)
	PulsarTopic            = os.Getenv("PULSAR_TOPIC")
	PulsarMessageSize      = util.GetEnvInt("PULSAR_MESSAGE_SIZE", 1024)
	PulsarSubscriptionName = util.GetEnvStr("PULSAR_SUBSCRIPTION_NAME", "my-sub")
	PulsarSubscriptionType = SubTypeEnum[util.GetEnvStr("PULSAR_SUBSCRIPTION_TYPE", Failover)]
)

// kafka server environment config
var (
	KafkaHost            = util.GetEnvStr("KAFKA_HOST", "localhost")
	KafkaPort            = util.GetEnvInt("KAFKA_PORT", 9092)
	KafkaTopic           = util.GetEnvStr("KAFKA_TOPIC", "testTopic")
	KafkaAutoOffsetReset = util.GetEnvStr("AUTO_OFFSET_RESET_CONFIG", "latest")
	KafkaGroupID         = os.Getenv("KAFKA_GROUP_ID")
	KafkaClient          = util.GetEnvStr("KAFKA_CLIENT", KafkaClientSarama)
	KafkaSaslEnable      = util.GetEnvBool("KAFKA_SASL_ENABLE", false)
	KafkaSaslUsername    = os.Getenv("KAFKA_SASL_USERNAME")
	KafkaSaslPassword    = os.Getenv("KAFKA_SASL_PASSWORD")
)
