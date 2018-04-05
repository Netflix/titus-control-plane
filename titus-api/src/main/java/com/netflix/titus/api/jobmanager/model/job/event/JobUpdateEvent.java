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

package com.netflix.titus.api.jobmanager.model.job.event;

import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;

public class JobUpdateEvent extends JobManagerEvent<Job> {
    private JobUpdateEvent(Job current, Optional<Job> previous) {
        super(current, previous);
    }

    @Override
    public String toString() {
        return "JobUpdateEvent{" +
                "current=" + getCurrent() +
                ", previous=" + getPrevious() +
                "}";
    }

    public static JobUpdateEvent newJob(Job current) {
        return new JobUpdateEvent(current, Optional.empty());
    }

    public static JobUpdateEvent jobChange(Job current, Job previous) {
        return new JobUpdateEvent(current, Optional.of(previous));
    }
}
