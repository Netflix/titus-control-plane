package com.netflix.titus.runtime.service;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import com.netflix.titus.grpc.protogen.ActivityQueryResult;
import com.netflix.titus.grpc.protogen.JobId;
import rx.Observable;

public interface JobActivityHistoryService {
    Observable<ActivityQueryResult> viewScalingActivities(JobId jobId, CallMetadata callMetadata);
}
