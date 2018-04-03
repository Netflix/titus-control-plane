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

package com.netflix.titus.api.model.event;

/**
 * Job state change event.
 */
public class JobStateChangeEvent<SOURCE> extends SchedulingEvent<SOURCE> {

    public enum JobState {Created, Resized, Activated, Deactivated, DisabledIncreaseDesired, DisabledDecreaseDesired, Finished}

    private final JobState jobState;

    public JobStateChangeEvent(String jobId, JobState jobState, long timestamp, SOURCE source) {
        super(jobId, timestamp, source);
        this.jobState = jobState;
    }

    public JobState getJobState() {
        return jobState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        JobStateChangeEvent that = (JobStateChangeEvent) o;

        return jobState == that.jobState;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (jobState != null ? jobState.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "JobStateChangeEvent{" +
                "jobId='" + getJobId() + '\'' +
                ", jobState=" + jobState +
                ", timestamp=" + getTimestamp() +
                ", sourceType=" + getSource().getClass() +
                '}';
    }
}
