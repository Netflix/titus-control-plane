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

public abstract class JobManagerEvent<TYPE> {

    private static final SnapshotMarkerEvent SNAPSHOT_MARKER = new SnapshotMarkerEvent();

    private final TYPE current;
    private final Optional<TYPE> previous;

    protected JobManagerEvent(TYPE current, Optional<TYPE> previous) {
        this.current = current;
        this.previous = previous;
    }

    public TYPE getCurrent() {
        return current;
    }

    public Optional<TYPE> getPrevious() {
        return previous;
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

    private static class SnapshotMarkerEvent extends JobManagerEvent<Job> {

        private SnapshotMarkerEvent() {
            super(Job.newBuilder().build(), Optional.empty());
        }
    }
}
