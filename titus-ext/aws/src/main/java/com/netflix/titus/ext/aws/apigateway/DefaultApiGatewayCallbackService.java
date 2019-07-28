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
package com.netflix.titus.ext.aws.apigateway;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.protobuf.UInt32Value;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.Job;
import com.netflix.titus.grpc.protogen.JobCapacityUpdateWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobCapacityWithOptionalAttributes;
import com.netflix.titus.grpc.protogen.JobStatus;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.grpc.protogen.ServiceJobSpec;
import com.netflix.titus.grpc.protogen.TaskQuery;
import com.netflix.titus.runtime.jobmanager.gateway.JobServiceGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Singleton
public class DefaultApiGatewayCallbackService implements ApiGatewayCallbackService {
    private static final Logger logger = LoggerFactory.getLogger(DefaultApiGatewayCallbackService.class);
    private JobServiceGateway jobServiceGateway;
    private static final String DIMENSION_NAME = "custom-resource:ResourceType:Property";

    @Inject
    public DefaultApiGatewayCallbackService(JobServiceGateway jobServiceGateway) {
        this.jobServiceGateway = jobServiceGateway;
    }

    @Override
    public Observable<ScalingPayload> getJobInstances(String jobId) {
        TaskQuery taskQuery = TaskQuery.newBuilder()
                .putFilteringCriteria("jobIds", jobId)
                .putFilteringCriteria("taskStates", "Started")
                .setPage(Page.newBuilder().setPageSize(1).build()).build();

        return jobServiceGateway.findTasks(taskQuery)
                .map(taskQueryResult -> taskQueryResult.getPagination().getTotalItems())
                .flatMap(numStartedTasks -> jobServiceGateway.findJob(jobId).map(job -> Pair.of(job, numStartedTasks)))
                .flatMap(jobTasksPair -> {
                    Job job = jobTasksPair.getLeft();
                    if (!job.getJobDescriptor().hasService()) {
                        return Observable.error(JobManagerException.notServiceJob(jobId));
                    }
                    ServiceJobSpec jobSpec = job.getJobDescriptor().getService();

                    ScalingPayload scalingPayload = new ScalingPayload();
                    scalingPayload.setActualCapacity(jobTasksPair.getRight());
                    scalingPayload.setDesiredCapacity(jobSpec.getCapacity().getDesired());
                    scalingPayload.setDimensionName(DIMENSION_NAME);
                    scalingPayload.setResourceName(jobId);
                    scalingPayload.setScalableTargetDimensionId(jobId);
                    scalingPayload.setVersion(getAcceptedTimestamp(job));
                    return Observable.just(scalingPayload);
                });
    }

    @Override
    public Observable<ScalingPayload> setJobInstances(String jobId, ScalingPayload scalingPayload) {
        JobCapacityWithOptionalAttributes jobCapacityWithOptionalAttributes = JobCapacityWithOptionalAttributes.newBuilder()
                .setDesired(UInt32Value.newBuilder().setValue(scalingPayload.getDesiredCapacity()).build()).build();
        JobCapacityUpdateWithOptionalAttributes jobCapacityRequest = JobCapacityUpdateWithOptionalAttributes.newBuilder()
                .setJobId(jobId)
                .setJobCapacityWithOptionalAttributes(jobCapacityWithOptionalAttributes).build();
        return jobServiceGateway.updateJobCapacityWithOptionalAttributes(jobCapacityRequest)
                .andThen(getJobInstances(jobId));
    }

    private String getAcceptedTimestamp(Job job) {
        List<JobStatus> jobStatusList = new ArrayList<>(job.getStatusHistoryList());
        jobStatusList.add(job.getStatus());

        Optional<String> timeStampStr = jobStatusList.stream()
                .filter(jobStatus -> jobStatus.getState() == JobStatus.JobState.Accepted)
                .findFirst()
                .map(jobStatus -> String.valueOf(jobStatus.getTimestamp()));

        if (timeStampStr.isPresent()) {
            return timeStampStr.get();
        }
        // Returning NoVersion should not have any negative side effect since
        // application auto scaling won't actually need this value to
        // differentiate between Titus Jobs, because we don't reuse Job IDs.
        logger.error("Titus Job {} is missing an Accepted timestamp!", job);
        return "NoVersion";
    }
}
