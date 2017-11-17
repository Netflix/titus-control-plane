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

package io.netflix.titus.api.endpoint.v2.rest.representation;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import io.netflix.titus.api.model.v2.JobCompletedReason;
import io.netflix.titus.api.model.v2.V2JobState;
import io.netflix.titus.common.util.CollectionsExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static java.util.Collections.emptySet;

/**
 */
public enum TitusTaskState {

    QUEUED, DISPATCHED, STARTING, RUNNING, FINISHED, ERROR, STOPPED, CRASHED, FAILED, UNKNOWN;

    public static final Set<TitusTaskState> ANY = Collections.unmodifiableSet(asSet(QUEUED, DISPATCHED, STARTING, RUNNING, FINISHED, ERROR, STOPPED, CRASHED, FAILED));
    public static final Set<TitusTaskState> ACTIVE = Collections.unmodifiableSet(asSet(QUEUED, DISPATCHED, STARTING, RUNNING));

    private static final Map<TitusTaskState, Set<TitusTaskState>> VALID_STATE_TRANSITIONS = CollectionsExt.<TitusTaskState, Set<TitusTaskState>>newHashMap()
            .entry(QUEUED, asSet(DISPATCHED, STARTING, RUNNING, FINISHED, ERROR, STOPPED, CRASHED, FAILED))
            .entry(DISPATCHED, asSet(STARTING, RUNNING, FINISHED, ERROR, STOPPED, CRASHED, FAILED))
            .entry(STARTING, asSet(RUNNING, FINISHED, ERROR, STOPPED, CRASHED, FAILED))
            .entry(RUNNING, asSet(FINISHED, ERROR, STOPPED, CRASHED, FAILED))
            .entry(FINISHED, emptySet())
            .entry(ERROR, emptySet())
            .entry(STOPPED, emptySet())
            .entry(CRASHED, emptySet())
            .entry(FAILED, emptySet())
            .toMap();
    private static Logger logger = LoggerFactory.getLogger(TitusTaskState.class);

    public static boolean isValidTransition(TitusTaskState from, TitusTaskState to) {
        return VALID_STATE_TRANSITIONS.get(from).contains(to);
    }

    public static boolean isBefore(TitusTaskState current, TitusTaskState reference) {
        if (current == reference) {
            return false;
        }
        return isValidTransition(current, reference);
    }

    public static TitusTaskState toTitusState(String state) {
        try {
            return valueOf(state);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public static V2JobState getV2State(TitusTaskState state) {
        switch (state) {
            case QUEUED:
                return V2JobState.Accepted;
            case DISPATCHED:
                return V2JobState.Launched;
            case STARTING:
                return V2JobState.StartInitiated;
            case RUNNING:
                return V2JobState.Started;
            case FINISHED:
                return V2JobState.Completed;
            case ERROR:
            case FAILED:
            case STOPPED:
            case CRASHED:
                return V2JobState.Failed;
            default:
                logger.debug("Unknown TitusState " + state + " to convert to V2State");
                return V2JobState.Noop;
        }
    }

    public static TitusTaskState getTitusState(V2JobState state, JobCompletedReason reason) {
        switch (state) {
            case Accepted:
                return QUEUED;
            case Launched:
                return DISPATCHED;
            case StartInitiated:
                return STARTING;
            case Started:
                return RUNNING;
            case Completed:
                return FINISHED;
            case Failed:
                switch (reason) {
                    case Error:
                        return ERROR;
                    case Failed:
                        return FAILED;
                    case Killed:
                        return STOPPED;
                    case Lost:
                        return CRASHED;
                    default:
                        return FAILED;
                }
            default:
                return UNKNOWN;
        }
    }

    public boolean isActive() {
        return ACTIVE.contains(this);
    }
}
