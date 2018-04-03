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

package com.netflix.titus.master.store;

import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.api.store.v2.V2StageMetadata;
import com.netflix.titus.api.store.v2.V2WorkerMetadata;

public class MetadataUtils {

    public static long getLastActivityTime(V2JobMetadata mjmd) {
        long c = 0L;
        for (V2StageMetadata msmd : mjmd.getStageMetadata()) {
            for (V2WorkerMetadata mwmd : msmd.getAllWorkers()) {
                long tm = 0L;
                switch (mwmd.getState()) {
                    case Accepted:
                        tm = mwmd.getAcceptedAt();
                        break;
                    case Completed:
                    case Failed:
                        tm = mwmd.getCompletedAt();
                        break;
                    case Launched:
                        tm = mwmd.getLaunchedAt();
                        break;
                    case Started:
                        tm = mwmd.getStartedAt();
                        break;
                    case StartInitiated:
                        tm = mwmd.getStartingAt();
                        break;
                }
                c = Math.max(c, tm);
            }
        }
        return c;
    }

    public static long getLastTerminationTime(V2JobMetadata mjmd) {
        long c = 0L;
        for (V2StageMetadata msmd : mjmd.getStageMetadata()) {
            for (V2WorkerMetadata mwmd : msmd.getAllWorkers()) {
                if (V2JobState.isTerminalState(mwmd.getState())) {
                    c = Math.max(c, mwmd.getCompletedAt());
                }
            }
        }
        return c;
    }
}
