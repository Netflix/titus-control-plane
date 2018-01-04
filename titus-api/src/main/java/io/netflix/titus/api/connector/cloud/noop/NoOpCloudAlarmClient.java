/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.connector.cloud.noop;

import io.netflix.titus.api.appscale.model.AlarmConfiguration;
import io.netflix.titus.api.connector.cloud.CloudAlarmClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

import java.util.List;

public class NoOpCloudAlarmClient implements CloudAlarmClient {
    private static Logger log = LoggerFactory.getLogger(NoOpCloudAlarmClient.class);

    @Override
    public Observable<String> createOrUpdateAlarm(String policyRefId, String jobId,
                                                  AlarmConfiguration alarmConfiguration,
                                                  String autoScalingGroup,
                                                  List<String> actions) {
        String alarmName = buildAlarmName(policyRefId, jobId);
        log.info("Created Alarm {}", alarmName);
        return Observable.just(alarmName);
    }

    @Override
    public Completable deleteAlarm(String policyRefId, String jobId) {
        log.info("Deleted Alarm {}", buildAlarmName(policyRefId, jobId));
        return Completable.complete();
    }

    private String buildAlarmName(String policyRefId, String jobId) {
        return String.format("%s/%s", jobId, policyRefId);
    }
}
