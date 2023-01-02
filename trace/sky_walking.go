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

package trace

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/SkyAPM/go2sky"
	"github.com/SkyAPM/go2sky/propagation"
	"github.com/SkyAPM/go2sky/reporter"
	"github.com/sirupsen/logrus"
	spantype "skywalking.apache.org/repo/goapi/collect/language/agent/v3"
	"time"
)

type SkyGo struct {
	Host       string
	Port       int
	SampleRate float64
	Enable     bool
}

var _ Tracing = (*SkyGo)(nil)

type SkyGoCarrier struct {
	Carrier *propagation.SpanContext
	Message []byte
}

func (carrier *SkyGoCarrier) Marshal() []byte {
	if len(carrier.Message) == 0 || carrier.Carrier == nil {
		logrus.Warn("reporter enable but carrier not set")
		return []byte("{}")
	}

	b, err := json.Marshal(carrier)
	if err != nil {
		logrus.Errorf("marshal body failed: %v", err)
		return []byte("{}")
	}
	return b
}

func (carrier *SkyGoCarrier) Unmarshal(b []byte) error {
	return json.Unmarshal(b, carrier)
}

func (carrier *SkyGoCarrier) GetHeaders() *Headers {
	headers := NewHeaders()
	sw8 := carrier.Carrier.EncodeSW8()
	correlation := carrier.Carrier.EncodeSW8Correlation()
	headers.Set(propagation.Header, sw8)
	headers.Set(propagation.HeaderCorrelation, correlation)
	return headers
}

func (sg *SkyGo) NewProvider() {
	if !sg.Enable {
		logrus.Warn("skywalking disabled")
		return
	}
	grpcReporter, err := reporter.NewGRPCReporter(fmt.Sprintf("%s:%d", sg.Host, sg.Port))
	if err != nil {
		logrus.Errorf("skywalking init error: %v", err)
		return
	}
	tracer, err := go2sky.NewTracer("kafka_go_pulsar", go2sky.WithReporter(grpcReporter),
		go2sky.WithSampler(sg.SampleRate), go2sky.WithInstance(instanceName()))
	if err != nil {
		logrus.Errorf("skywalking init error: %v", err)
		return
	}
	go2sky.SetGlobalTracer(tracer)
}

func (sg *SkyGo) NewMqSpan(ctx context.Context, operationName, peer, log string, headers *Headers) {
	if !sg.Enable {
		return
	}

	span, _, err := go2sky.GetGlobalTracer().CreateEntrySpan(ctx, operationName, func(k string) (string, error) {
		_, val := headers.Get(k)
		return val, nil
	})
	if err != nil {
		logrus.Errorf("create span failed: %v", err)
		return
	}
	span.SetSpanLayer(spantype.SpanLayer_MQ)
	span.Log(time.Now(), log)
	span.End()
}
