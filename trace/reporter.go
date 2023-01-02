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
	"fmt"
	"github.com/SkyAPM/go2sky/propagation"
	"github.com/hashicorp/go-uuid"
	"os"
)

type Headers map[string]string

func NewHeaders() *Headers {
	var instance = new(Headers)
	*instance = make(map[string]string)
	return instance
}

func (headers *Headers) Set(k, v string) {
	(*headers)[k] = v
}

func (headers *Headers) Get(k string) (bool, string) {
	v, has := (*headers)[k]
	return has, v
}

func (headers *Headers) SkyGoHeaders() (*SkyGoCarrier, error) {
	var sw8, correlation string
	var exist bool
	if exist, sw8 = headers.Get(propagation.Header); !exist {
		return nil, fmt.Errorf("sw8 not found")
	}

	if exist, correlation = headers.Get(propagation.HeaderCorrelation); !exist {
		return nil, fmt.Errorf("sw8-correlation not found")
	}

	var ctx = new(propagation.SpanContext)
	if err := ctx.DecodeSW8(sw8); err != nil {
		return nil, err
	}
	if err := ctx.DecodeSW8Correlation(correlation); err != nil {
		return nil, err
	}
	return &SkyGoCarrier{
		Carrier: ctx,
	}, nil
}

type Tracing interface {
	// NewProvider new reporter prodiver
	NewProvider()
	// NewMqSpan create span and inject header to context
	// Example: NewMqSpan(ctx, "/Service/User/Login", "UserService", headers)
	// 	traceInfo := headers.SkyGoHeaders()
	NewMqSpan(ctx context.Context, operationName, peer, log string, headers *Headers)
}

func instanceName() string {
	hostname, err := os.Hostname()
	if err == nil {
		return hostname
	}
	generateUUID, err := uuid.GenerateUUID()
	if err == nil {
		return generateUUID
	}
	return "perf-mq-producer-go"
}
