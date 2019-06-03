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

package com.netflix.titus.supplementary.jobactivity.endpoint.grpc;

import javax.inject.Singleton;

import com.netflix.titus.grpc.protogen.JobActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.TaskActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobActivityServiceGrpc;
import com.netflix.titus.grpc.protogen.TaskId;
import com.netflix.titus.supplementary.jobactivity.endpoint.TestData;
import io.grpc.stub.StreamObserver;

@Singleton
public class JobActivityGrpcService extends JobActivityServiceGrpc.JobActivityServiceImplBase {

    @Override
    public void getJobActivityRecords(JobId jobId, StreamObserver<JobActivityQueryResult> responseObserver) {
        responseObserver.onNext(TestData.newJobActivityQueryResult());
        responseObserver.onCompleted();
    }

    @Override
    public void getTaskActivityRecords(TaskId taskId, StreamObserver<TaskActivityQueryResult> responseObserver) {
        //responseObserver.onNext(TestData.newTaskActivityQueryResult());
        responseObserver.onCompleted();
    }
}
