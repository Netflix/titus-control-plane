package com.netflix.titus.ext.aws.apigateway;

import rx.Observable;

public interface AwsGatewayCallbackService {
    Observable<ScalingPayload> getJobInstances(String jobId);

    Observable<ScalingPayload> setJobInstances(String jobId, ScalingPayload scalingPayload);
}
