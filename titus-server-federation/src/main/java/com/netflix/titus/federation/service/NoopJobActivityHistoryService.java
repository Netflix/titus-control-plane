package com.netflix.titus.federation.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.grpc.protogen.ActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.runtime.service.JobActivityHistoryService;
import rx.Observable;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class NoopJobActivityHistoryService implements JobActivityHistoryService {
    @Inject
    public NoopJobActivityHistoryService() {
    }

    @Override
    public Observable<ActivityQueryResult> viewScalingActivities(JobId jobId, CallMetadata callMetadata) {
        return null;
    }
}