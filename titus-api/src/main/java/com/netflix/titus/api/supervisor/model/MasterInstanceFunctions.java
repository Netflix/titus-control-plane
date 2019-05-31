/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.supervisor.model;

import java.util.List;

import com.netflix.titus.common.util.CollectionsExt;

public final class MasterInstanceFunctions {

    private static final int MAX_STATUS_HISTORY_SIZE = 20;

    private MasterInstanceFunctions() {
    }

    public static MasterInstance moveTo(MasterInstance masterInstance, MasterStatus nextStatus) {
        List<MasterStatus> statusHistory = CollectionsExt.copyAndAdd(masterInstance.getStatusHistory(), masterInstance.getStatus());
        if (statusHistory.size() > MAX_STATUS_HISTORY_SIZE) {
            statusHistory = statusHistory.subList(statusHistory.size() - MAX_STATUS_HISTORY_SIZE, statusHistory.size());
        }

        return masterInstance.toBuilder()
                .withStatus(nextStatus)
                .withStatusHistory(statusHistory)
                .build();
    }

    public static boolean areDifferent(MasterInstance first, MasterInstance second) {
        if (first == null) {
            return second != null;
        }
        if (second == null) {
            return true;
        }
        return !clearTimestamp(first).equals(clearTimestamp(second));
    }

    public static boolean areDifferent(MasterStatus first, MasterStatus second) {
        if (first == null) {
            return second != null;
        }
        if (second == null) {
            return true;
        }
        return !clearTimestamp(first).equals(clearTimestamp(second));
    }

    public static boolean areDifferent(ReadinessStatus first, ReadinessStatus second) {
        if (first == null) {
            return second != null;
        }
        if (second == null) {
            return true;
        }
        return !clearTimestamp(first).equals(clearTimestamp(second));
    }

    private static MasterInstance clearTimestamp(MasterInstance instance) {
        return instance.toBuilder().withStatus(instance.getStatus().toBuilder().withTimestamp(0).build()).build();
    }

    private static MasterStatus clearTimestamp(MasterStatus status) {
        return status.toBuilder().withTimestamp(0).build();
    }

    private static ReadinessStatus clearTimestamp(ReadinessStatus first) {
        return first.toBuilder().withTimestamp(0).build();
    }
}
