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

import java.util.Objects;
import java.util.Optional;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.api.jobmanager.service.JobManagerConstants;
import com.netflix.titus.api.model.callmetadata.CallMetadata;

public abstract class JobManagerEvent<TYPE> {

    private static final SnapshotMarkerEvent SNAPSHOT_MARKER = new SnapshotMarkerEvent();
    private static final KeepAliveEvent KEEP_ALIVE_EVENT = new KeepAliveEvent();

    private final TYPE current;
    private final Optional<TYPE> previous;
    private final CallMetadata callMetadata;

    protected JobManagerEvent(TYPE current, Optional<TYPE> previous, CallMetadata callMetadata) {
        this.current = current;
        this.previous = previous;
        this.callMetadata = callMetadata;
    }

    public TYPE getCurrent() {
        return current;
    }

    public Optional<TYPE> getPrevious() {
        return previous;
    }

    public CallMetadata getCallMetadata() {
        if (callMetadata == null) {
            return JobManagerConstants.UNDEFINED_CALL_METADATA;
        }
        return callMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof JobManagerEvent)) {
            return false;
        }
        JobManagerEvent<?> that = (JobManagerEvent<?>) o;
        return current.equals(that.current) &&
                previous.equals(that.previous);
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, previous);
    }

    public static JobManagerEvent<Job> snapshotMarker() {
        return SNAPSHOT_MARKER;
    }

    public static JobManagerEvent<Job> keepAliveEvent() {
        return KEEP_ALIVE_EVENT;
    }

    private static class SnapshotMarkerEvent extends JobManagerEvent<Job> {

        private SnapshotMarkerEvent() {
            super(Job.newBuilder().build(), Optional.empty(), CallMetadata.newBuilder().withCallerId("SnapshotMarkerEvent").withCallReason("initializing").build());
        }
    }

    private static class KeepAliveEvent extends JobManagerEvent<Job> {

        private KeepAliveEvent() {
            super(Job.newBuilder().build(), Optional.empty(), CallMetadata.newBuilder().withCallerId("KeepAliveEvent").withCallReason("keep alive event").build());
        }
    }
}
