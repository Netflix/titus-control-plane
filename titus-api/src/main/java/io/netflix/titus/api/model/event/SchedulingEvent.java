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

package io.netflix.titus.api.model.event;

/**
 * A base class for all scheduler events related to a particular job.
 */
public abstract class SchedulingEvent<SOURCE> {

    private final String jobId;
    private final long timestamp;
    private final SOURCE source;

    protected SchedulingEvent(String jobId, long timestamp, SOURCE source) {
        this.jobId = jobId;
        this.timestamp = timestamp;
        this.source = source;
    }

    public String getJobId() {
        return jobId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public SOURCE getSource() {
        return source;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SchedulingEvent<?> that = (SchedulingEvent<?>) o;

        if (timestamp != that.timestamp) {
            return false;
        }
        if (jobId != null ? !jobId.equals(that.jobId) : that.jobId != null) {
            return false;
        }
        return source != null ? source.equals(that.source) : that.source == null;
    }

    @Override
    public int hashCode() {
        int result = jobId != null ? jobId.hashCode() : 0;
        result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (source != null ? source.hashCode() : 0);
        return result;
    }
}
