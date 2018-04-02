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

package com.netflix.titus.api.endpoint.v2.rest.representation;

import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.V2JobState;

public enum TitusJobState {
    QUEUED, DISPATCHED, FINISHED, FAILED, UNKNOWN;

    public static TitusJobState getTitusState(V2JobState state) {
        switch (state) {
            case Accepted:
                return QUEUED;
            case Launched:
                return DISPATCHED;
            case Completed:
                return FINISHED;
            case Failed:
                return FAILED;
            case StartInitiated:
            case Started:
            default:
                return UNKNOWN;
        }
    }

    public static boolean isActive(TitusJobState state) {
        return state == TitusJobState.QUEUED || state == TitusJobState.DISPATCHED;
    }
}
