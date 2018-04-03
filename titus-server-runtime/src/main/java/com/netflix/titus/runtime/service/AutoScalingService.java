/*
 * Copyright 2018 Netflix, Inc.
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

package com.netflix.titus.runtime.service;

import com.netflix.titus.grpc.protogen.DeletePolicyRequest;
import com.netflix.titus.grpc.protogen.GetPolicyResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.PutPolicyRequest;
import com.netflix.titus.grpc.protogen.ScalingPolicyID;
import com.netflix.titus.grpc.protogen.UpdatePolicyRequest;
import rx.Completable;
import rx.Observable;

public interface AutoScalingService {

    Observable<GetPolicyResult> getJobScalingPolicies(JobId jobId);

    Observable<ScalingPolicyID> setAutoScalingPolicy(PutPolicyRequest request);

    Observable<GetPolicyResult> getScalingPolicy(ScalingPolicyID request);

    Observable<GetPolicyResult> getAllScalingPolicies();

    Completable deleteAutoScalingPolicy(DeletePolicyRequest request);

    Completable updateAutoScalingPolicy(UpdatePolicyRequest request);
}
