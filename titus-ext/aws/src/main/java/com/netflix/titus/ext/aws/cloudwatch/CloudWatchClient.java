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

package com.netflix.titus.ext.aws.cloudwatch;

import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.DeleteAlarmsRequest;
import com.amazonaws.services.cloudwatch.model.DeleteAlarmsResult;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmResult;
import com.amazonaws.services.cloudwatch.model.ResourceNotFoundException;
import com.netflix.spectator.api.Counter;
import com.netflix.spectator.api.Registry;
import com.netflix.titus.api.appscale.model.AlarmConfiguration;
import com.netflix.titus.api.appscale.service.AutoScalePolicyException;
import com.netflix.titus.api.connector.cloud.CloudAlarmClient;
import com.netflix.titus.ext.aws.AwsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Emitter;
import rx.Observable;

import static com.netflix.titus.ext.aws.RetryWrapper.wrapWithExponentialRetry;


@Singleton
public class CloudWatchClient implements CloudAlarmClient {
    public static final String AUTO_SCALING_GROUP_NAME = "AutoScalingGroupName";
    private static Logger log = LoggerFactory.getLogger(CloudWatchClient.class);
    private final AmazonCloudWatchAsync awsCloudWatch;

    public static final String METRIC_CLOUD_WATCH_CREATE_ERROR = "titus.cloudwatch.create.error";
    public static final String METRIC_CLOUD_WATCH_CREATE_ALARM = "titus.cloudwatch.create.alarm";
    public static final String METRIC_CLOUD_WATCH_DELETE_ALARM = "titus.cloudwatch.delete.alarm";
    public static final String METRIC_CLOUD_WATCH_DELETE_ERROR = "titus.cloudwatch.delete.error";

    private final Counter createAlarmCounter;
    private final Counter deleteAlarmCounter;
    private final Counter deleteErrorCounter;
    private final Counter createErrorCounter;


    @Inject
    public CloudWatchClient(AWSCredentialsProvider awsCredentialsProvider,
                            AwsConfiguration awsConfiguration,
                            Registry registry) {

        awsCloudWatch = AmazonCloudWatchAsyncClientBuilder.standard().withCredentials(awsCredentialsProvider)
                .withRegion(awsConfiguration.getRegion()).build();

        createAlarmCounter = registry.counter(METRIC_CLOUD_WATCH_CREATE_ALARM);
        deleteAlarmCounter = registry.counter(METRIC_CLOUD_WATCH_DELETE_ALARM);
        deleteErrorCounter = registry.counter(METRIC_CLOUD_WATCH_DELETE_ERROR);
        createErrorCounter = registry.counter(METRIC_CLOUD_WATCH_CREATE_ERROR);
    }


    @Override
    public Observable<String> createOrUpdateAlarm(String policyRefId,
                                                  String jobId,
                                                  AlarmConfiguration alarmConfiguration,
                                                  String autoScalingGroup,
                                                  List<String> actions) {
        Dimension dimension = new Dimension();
        dimension.setName(AUTO_SCALING_GROUP_NAME);
        dimension.setValue(autoScalingGroup);

        String cloudWatchName = buildCloudWatchName(policyRefId, jobId);

        PutMetricAlarmRequest putMetricAlarmRequest = new PutMetricAlarmRequest();
        if (alarmConfiguration.getActionsEnabled().isPresent()) {
            putMetricAlarmRequest.setActionsEnabled(alarmConfiguration.getActionsEnabled().get());
        }
        putMetricAlarmRequest.setAlarmActions(actions);
        putMetricAlarmRequest.setAlarmName(cloudWatchName);
        putMetricAlarmRequest.setDimensions(Arrays.asList(dimension));
        putMetricAlarmRequest.setNamespace(alarmConfiguration.getMetricNamespace());
        putMetricAlarmRequest.setComparisonOperator(alarmConfiguration.getComparisonOperator().name());
        putMetricAlarmRequest.setStatistic(alarmConfiguration.getStatistic().name());
        putMetricAlarmRequest.setEvaluationPeriods(alarmConfiguration.getEvaluationPeriods());
        putMetricAlarmRequest.setPeriod(alarmConfiguration.getPeriodSec());
        putMetricAlarmRequest.setThreshold(alarmConfiguration.getThreshold());
        putMetricAlarmRequest.setMetricName(alarmConfiguration.getMetricName());

        return wrapWithExponentialRetry(String.format("createOrUpdateAlarm in policy %s for job %s", policyRefId, jobId),
                Observable.create(emitter ->
                        awsCloudWatch.putMetricAlarmAsync(putMetricAlarmRequest, new AsyncHandler<PutMetricAlarmRequest, PutMetricAlarmResult>() {
                            @Override
                            public void onError(Exception exception) {
                                createErrorCounter.increment();
                                emitter.onError(AutoScalePolicyException.errorCreatingAlarm(policyRefId, exception.getMessage()));
                            }

                            @Override
                            public void onSuccess(PutMetricAlarmRequest request, PutMetricAlarmResult putMetricAlarmResult) {
                                int httpStatusCode = putMetricAlarmResult.getSdkHttpMetadata().getHttpStatusCode();
                                log.info("Created Cloud Watch Alarm {} for {} - status {}", request, jobId, httpStatusCode);
                                // TODO : how to get ARN created by AWS for this resource ? returning cloudWatchName for now
                                createAlarmCounter.increment();
                                emitter.onNext(cloudWatchName);
                                emitter.onCompleted();
                            }
                        }), Emitter.BackpressureMode.NONE));
    }

    @Override
    public Completable deleteAlarm(String policyRefId, String jobId) {
        DeleteAlarmsRequest deleteAlarmsRequest = new DeleteAlarmsRequest();
        deleteAlarmsRequest.setAlarmNames(Arrays.asList(buildCloudWatchName(policyRefId, jobId)));


        return wrapWithExponentialRetry(String.format("deleteAlarm in policy %s for job %s", policyRefId, jobId),
                Observable.create(emitter ->
                        awsCloudWatch.deleteAlarmsAsync(deleteAlarmsRequest, new AsyncHandler<DeleteAlarmsRequest, DeleteAlarmsResult>() {
                            @Override
                            public void onError(Exception exception) {
                                deleteErrorCounter.increment();
                                if (exception instanceof ResourceNotFoundException) {
                                    emitter.onError(AutoScalePolicyException.unknownScalingPolicy(policyRefId, exception.getMessage()));
                                } else {
                                    emitter.onError(AutoScalePolicyException.errorDeletingAlarm(policyRefId, exception.getMessage()));
                                }
                            }

                            @Override
                            public void onSuccess(DeleteAlarmsRequest request, DeleteAlarmsResult deleteAlarmsResult) {
                                int httpStatusCode = deleteAlarmsResult.getSdkHttpMetadata().getHttpStatusCode();
                                log.info("Deleted cloud watch alarm for job-id {}, status {}", jobId, httpStatusCode);
                                deleteAlarmCounter.increment();
                                emitter.onCompleted();
                            }
                        }), Emitter.BackpressureMode.NONE)).toCompletable();
    }

    private String buildCloudWatchName(String policyRefId, String jobId) {
        return String.format("%s/%s", jobId, policyRefId);
    }


}
