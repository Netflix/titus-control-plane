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
package com.netflix.titus.supplementary.es.publish;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.netflix.titus.grpc.protogen.Task;
import com.netflix.titus.runtime.endpoint.common.EmptyLogStorageInfo;
import com.netflix.titus.runtime.endpoint.v3.grpc.V3GrpcModelConverters;
import com.netflix.titus.testkit.model.job.JobGenerator;

public class TestUtils {

    static List<Task> generateSampleTasks(int numTasks) {
        return IntStream.range(0, numTasks)
                .mapToObj(ignored -> V3GrpcModelConverters.toGrpcTask(JobGenerator.oneBatchTask(), new EmptyLogStorageInfo<>()))
                .collect(Collectors.toList());
    }
}
