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

package kafka

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
	"perf-mq-producer-go/conf"
	"sync"
	"time"
)

type iConsumer interface {
	initial(ctx context.Context)
	consume(ctx context.Context, topic string)
}

var _ iConsumer = (*kafkaGo)(nil)
var _ iConsumer = (*kafkaSarama)(nil)

type kafkaGo struct {
	reader *kafka.Reader
}

func (kg *kafkaGo) initial(ctx context.Context) {
	dialer := kafka.DefaultDialer
	if conf.KafkaSaslEnable {
		dialer = &kafka.Dialer{
			ClientID: "pf-mq",
			Timeout:  time.Second * 3,
			SASLMechanism: plain.Mechanism{
				Username: conf.KafkaSaslUsername,
				Password: conf.KafkaSaslPassword,
			},
		}
	}
	var offsetMode int64
	switch conf.KafkaAutoOffsetReset {
	case "latest":
		offsetMode = kafka.LastOffset
	default:
		offsetMode = kafka.FirstOffset
	}
	kg.reader = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{fmt.Sprintf("%s:%d", conf.KafkaHost, conf.KafkaPort)},
		GroupID:     conf.KafkaGroupID,
		GroupTopics: nil,
		Topic:       conf.KafkaTopic,
		Dialer:      dialer,
		MinBytes:    1024 * 10,    // 10kb
		MaxBytes:    1024 * 10000, // 10mb
		StartOffset: offsetMode,
	})
}

func (kg *kafkaGo) consume(ctx context.Context, topic string) {
	defer kg.reader.Close()
	for {
		message, err := kg.reader.ReadMessage(ctx)
		if err != nil {
			break
		}
		logrus.Infof("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}

type kafkaSarama struct {
	reader sarama.Consumer
}

func (ks *kafkaSarama) initial(ctx context.Context) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	if conf.KafkaSaslEnable {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		config.Net.SASL.User = conf.KafkaSaslUsername
		config.Net.SASL.Password = conf.KafkaSaslPassword
	}
	consumer, err := sarama.NewConsumer([]string{fmt.Sprintf("%s:%d",
		conf.KafkaHost, conf.KafkaPort)}, config)
	if err != nil {
		logrus.Fatalf("init consumer failed: %+v", err)
	}
	ks.reader = consumer
}

func (ks *kafkaSarama) consume(ctx context.Context, topic string) {
	defer ks.reader.Close()
	// count topic partition
	partitions, err := ks.reader.Partitions(topic)
	if err != nil {
		logrus.Errorf("count topic partitions failed: %+v", err)
	}
	var wg sync.WaitGroup
	wg.Add(len(partitions))
	for _, partitionId := range partitions {
		go consumeSaramaByPartition(ks.reader, conf.KafkaTopic, partitionId, &wg)
	}
	wg.Wait()
}

func Start() {
	for i := 0; i < conf.RoutineNum; i++ {
		go startConsumer()
	}
}

func startConsumer() {
	var dialCtx = context.Background()
	var consumer iConsumer
	switch conf.KafkaClient {
	case conf.KafkaClientSarama:
		consumer = &kafkaSarama{}
	case conf.KafkaClientGo:
		consumer = &kafkaGo{}
	default:
		logrus.Errorf("unsupport client: %+v", conf.KafkaClient)
		return
	}
	consumer.initial(dialCtx)
	consumer.consume(dialCtx, conf.KafkaTopic)
}

func consumeSaramaByPartition(consumer sarama.Consumer, topic string, partitionId int32, wg *sync.WaitGroup) {
	defer wg.Done()
	var offsetMode int64
	switch conf.KafkaAutoOffsetReset {
	case "latest":
		offsetMode = sarama.OffsetNewest
	default:
		offsetMode = sarama.OffsetOldest
	}
	partitionConsumer, err := consumer.ConsumePartition(topic, partitionId, offsetMode)
	if err != nil {
		logrus.Errorf("consume partition failed: %+v", err)
		return
	}
	defer partitionConsumer.Close()
	for message := range partitionConsumer.Messages() {
		logrus.Infof("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, string(message.Value))
	}
}
