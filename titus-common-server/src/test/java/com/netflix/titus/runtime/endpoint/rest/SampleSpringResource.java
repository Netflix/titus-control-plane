/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.runtime.endpoint.rest;

import com.google.common.base.Preconditions;
import com.netflix.titus.testing.SampleGrpcService.SampleComplexMessage;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(path = "/test", produces = "application/json")
public class SampleSpringResource {

    private volatile SampleComplexMessage lastValue;

    @GetMapping
    public SampleComplexMessage getLast() {
        return Preconditions.checkNotNull(lastValue);
    }

    @GetMapping(path = "/error")
    public SampleComplexMessage getError() {
        throw new IllegalStateException("simulated error");
    }

    @PostMapping
    public ResponseEntity<Void> postLast(@RequestBody SampleComplexMessage body) {
        lastValue = body;
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }
}
