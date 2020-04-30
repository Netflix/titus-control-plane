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

package com.netflix.titus.ext.jobvalidator.s3;

import java.time.Duration;

import com.netflix.compute.validator.protogen.ValidationServiceGrpc;
import com.netflix.titus.common.util.grpc.reactor.client.ReactorToGrpcClientBuilder;
import io.grpc.Channel;
import org.junit.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;

public class ReactorValidationServiceClientTest {

    @Test
    public void testMatchesGrpcApi() {
        ReactorValidationServiceClient client = ReactorToGrpcClientBuilder
                .newBuilderWithDefaults(ReactorValidationServiceClient.class,
                        ValidationServiceGrpc.newStub(Mockito.mock(Channel.class)),
                        ValidationServiceGrpc.getServiceDescriptor(),
                        Void.class
                )
                .withTimeout(Duration.ofSeconds(1))
                .build();
        assertThat(client).isNotNull();
    }
}