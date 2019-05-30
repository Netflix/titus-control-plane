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

package com.netflix.titus.supplementary.jobactivity.endpoint;

import com.netflix.titus.grpc.protogen.CallMetadata;
import com.netflix.titus.grpc.protogen.JobActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobActivityRecord;

/**
 * TODO Remove when REST/GRPC endpoints fully implemented.
 */
public class TestData {

    public static JobActivityQueryResult newJobActivityQueryResult() {
        return JobActivityQueryResult.newBuilder()
                .addRecords(JobActivityRecord.newBuilder()
                        .setCallMetadata(
                                CallMetadata.newBuilder()
                                        .setCallerId("test")
                                        .addCallPath("test")
                                        .build()
                        )
                        .setTimestamp(System.currentTimeMillis())
                        .build()
                )
                .build();
    }

    public static TaskActivityQueryResult newTaskActivityQueryResult() {
        return TaskActivityQueryResult.newBuilder();
    }
}
