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

package com.netflix.titus.ext.aws.appscale;

import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.http.HttpResponse;
import com.amazonaws.http.SdkHttpMetadata;
import com.amazonaws.services.applicationautoscaling.AWSApplicationAutoScalingAsync;
import com.amazonaws.services.applicationautoscaling.model.DeleteScalingPolicyRequest;
import com.amazonaws.services.applicationautoscaling.model.DeleteScalingPolicyResult;
import com.amazonaws.services.applicationautoscaling.model.DeregisterScalableTargetRequest;
import com.amazonaws.services.applicationautoscaling.model.DeregisterScalableTargetResult;
import com.amazonaws.services.applicationautoscaling.model.ObjectNotFoundException;
import com.netflix.spectator.api.NoopRegistry;
import javaslang.concurrent.Future;
import org.junit.Test;
import rx.observers.AssertableSubscriber;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AWSAppAutoScalingClientTest {

    @Test
    public void deleteScalingPolicyIsIdempotent() {
        String jobId = UUID.randomUUID().toString();
        String policyId = UUID.randomUUID().toString();

        AWSApplicationAutoScalingAsync clientAsync = mock(AWSApplicationAutoScalingAsync.class);
        AWSAppScalingConfig config = mock(AWSAppScalingConfig.class);
        AWSAppAutoScalingClient autoScalingClient = new AWSAppAutoScalingClient(clientAsync, config, new NoopRegistry());

        // delete happens successfully on the first attempt
        AtomicBoolean isDeleted = new AtomicBoolean(false);
        when(clientAsync.deleteScalingPolicyAsync(any(), any())).thenAnswer(invocation -> {
            DeleteScalingPolicyRequest request = invocation.getArgument(0);
            AsyncHandler<DeleteScalingPolicyRequest, DeleteScalingPolicyResult> handler = invocation.getArgument(1);

            if (isDeleted.get()) {
                ObjectNotFoundException notFoundException = new ObjectNotFoundException(policyId + " does not exist");
                handler.onError(notFoundException);
                return Future.failed(notFoundException);
            }

            DeleteScalingPolicyResult resultSuccess = new DeleteScalingPolicyResult();
            HttpResponse successResponse = new HttpResponse(null, null);
            successResponse.setStatusCode(200);
            resultSuccess.setSdkHttpMetadata(SdkHttpMetadata.from(successResponse));

            isDeleted.set(true);
            handler.onSuccess(request, resultSuccess);
            return Future.successful(resultSuccess);
        });

        AssertableSubscriber<Void> firstCall = autoScalingClient.deleteScalingPolicy(policyId, jobId).test();
        firstCall.awaitTerminalEvent(2, TimeUnit.SECONDS);
        firstCall.assertNoErrors();
        firstCall.assertCompleted();
        verify(clientAsync, times(1)).deleteScalingPolicyAsync(any(), any());

        // second should complete fast when NotFound and not retry with exponential backoff
        AssertableSubscriber<Void> secondCall = autoScalingClient.deleteScalingPolicy(policyId, jobId).test();
        secondCall.awaitTerminalEvent(2, TimeUnit.SECONDS);
        secondCall.assertNoErrors();
        secondCall.assertCompleted();
        verify(clientAsync, times(2)).deleteScalingPolicyAsync(any(), any());
    }

    @Test
    public void deleteScalableTargetIsIdempotent() {
        String jobId = UUID.randomUUID().toString();
        String policyId = UUID.randomUUID().toString();

        AWSApplicationAutoScalingAsync clientAsync = mock(AWSApplicationAutoScalingAsync.class);
        AWSAppScalingConfig config = mock(AWSAppScalingConfig.class);
        AWSAppAutoScalingClient autoScalingClient = new AWSAppAutoScalingClient(clientAsync, config, new NoopRegistry());

        AtomicBoolean isDeleted = new AtomicBoolean(false);
        when(clientAsync.deregisterScalableTargetAsync(any(), any())).thenAnswer(invocation -> {
            DeregisterScalableTargetRequest request = invocation.getArgument(0);
            AsyncHandler<DeregisterScalableTargetRequest, DeregisterScalableTargetResult> handler = invocation.getArgument(1);
            if (isDeleted.get()) {
                ObjectNotFoundException notFoundException = new ObjectNotFoundException(policyId + " does not exist");
                handler.onError(notFoundException);
                return Future.failed(notFoundException);
            }

            DeregisterScalableTargetResult resultSuccess = new DeregisterScalableTargetResult();
            HttpResponse successResponse = new HttpResponse(null, null);
            successResponse.setStatusCode(200);
            resultSuccess.setSdkHttpMetadata(SdkHttpMetadata.from(successResponse));

            isDeleted.set(true);
            handler.onSuccess(request, resultSuccess);
            return Future.successful(resultSuccess);
        });

        AssertableSubscriber<Void> firstCall = autoScalingClient.deleteScalableTarget(jobId).test();
        firstCall.awaitTerminalEvent(2, TimeUnit.SECONDS);
        firstCall.assertNoErrors();
        firstCall.assertCompleted();
        verify(clientAsync, times(1)).deregisterScalableTargetAsync(any(), any());

        // second should complete fast when NotFound and not retry with exponential backoff
        AssertableSubscriber<Void> secondCall = autoScalingClient.deleteScalableTarget(jobId).test();
        secondCall.awaitTerminalEvent(2, TimeUnit.SECONDS);
        secondCall.assertNoErrors();
        secondCall.assertCompleted();
        verify(clientAsync, times(2)).deregisterScalableTargetAsync(any(), any());
    }

}
