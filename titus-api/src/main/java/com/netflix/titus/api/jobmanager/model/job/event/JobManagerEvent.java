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

    private final TYPE current;
    private final Optional<TYPE> previous;
    private final boolean archived;
    private final CallMetadata callMetadata;

    protected JobManagerEvent(TYPE current, Optional<TYPE> previous, boolean archived, CallMetadata callMetadata) {
        this.current = current;
        this.previous = previous;
        this.archived = archived;
        this.callMetadata = callMetadata == null ? JobManagerConstants.UNDEFINED_CALL_METADATA : callMetadata;
    }

    public TYPE getCurrent() {
        return current;
    }

    public Optional<TYPE> getPrevious() {
        return previous;
    }

    public boolean isArchived() {
        return archived;
    }

    public CallMetadata getCallMetadata() {
        return callMetadata;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobManagerEvent<?> that = (JobManagerEvent<?>) o;
        return archived == that.archived && Objects.equals(current, that.current) && Objects.equals(previous, that.previous) && Objects.equals(callMetadata, that.callMetadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(current, previous, archived, callMetadata);
    }

    public static JobManagerEvent<Job> snapshotMarker() {
        return SNAPSHOT_MARKER;
    }

    public static JobManagerEvent<Job> keepAliveEvent(long timestamp) {
        return new JobKeepAliveEvent(timestamp);
    }

    private static class SnapshotMarkerEvent extends JobManagerEvent<Job> {

        private SnapshotMarkerEvent() {
            super(Job.newBuilder().build(),
                    Optional.empty(),
                    false,
                    CallMetadata.newBuilder().withCallerId("SnapshotMarkerEvent").withCallReason("initializing").build()
            );
        }
    }
}
