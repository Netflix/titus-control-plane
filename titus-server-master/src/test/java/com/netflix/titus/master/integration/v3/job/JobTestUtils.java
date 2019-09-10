/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.master.integration.v3.job;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.rpc.BadRequest;
import com.netflix.titus.client.common.grpc.GrpcClientErrorUtils;
import com.netflix.titus.grpc.protogen.JobDescriptor;
import com.netflix.titus.grpc.protogen.JobManagementServiceGrpc;
import io.grpc.StatusRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class JobTestUtils {

    public static void submitBadJob(JobManagementServiceGrpc.JobManagementServiceBlockingStub client,
                                     JobDescriptor badJobDescriptor,
                                     String... expectedFields) {
        Set<String> expectedFieldSet = new HashSet<>();
        Collections.addAll(expectedFieldSet, expectedFields);

        try {
            client.createJob(badJobDescriptor).getId();
            fail("Expected test to fail");
        } catch (StatusRuntimeException e) {
            System.out.println("Received StatusRuntimeException: " + e.getMessage());

            Optional<BadRequest> badRequestOpt = GrpcClientErrorUtils.getDetail(e, BadRequest.class);

            // Print validation messages for visual inspection
            badRequestOpt.ifPresent(System.out::println);

            Set<String> badFields = badRequestOpt.map(badRequest ->
                    badRequest.getFieldViolationsList().stream().map(BadRequest.FieldViolation::getField).collect(Collectors.toSet())
            ).orElse(Collections.emptySet());

            assertThat(badFields).containsAll(expectedFieldSet);
            assertThat(badFields.size()).isEqualTo(expectedFieldSet.size());
        }
    }
}
