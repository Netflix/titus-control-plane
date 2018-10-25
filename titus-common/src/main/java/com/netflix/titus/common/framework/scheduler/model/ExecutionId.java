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

package com.netflix.titus.common.framework.scheduler.model;

import java.util.Objects;

public class ExecutionId {

    private static final ExecutionId INITIAL = new ExecutionId(1, 1, 1);

    private final int id;
    private final int attempt;
    private final int total;

    public ExecutionId(int id, int attempt, int total) {
        this.id = id;
        this.attempt = attempt;
        this.total = total;
    }

    public int getId() {
        return id;
    }

    public int getAttempt() {
        return attempt;
    }

    public int getTotal() {
        return total;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ExecutionId that = (ExecutionId) o;
        return id == that.id &&
                attempt == that.attempt &&
                total == that.total;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, attempt, total);
    }

    @Override
    public String toString() {
        return "Iteration{" +
                "id=" + id +
                ", attempt=" + attempt +
                ", total=" + total +
                '}';
    }

    public static ExecutionId initial() {
        return INITIAL;
    }

    public static ExecutionId nextIteration(ExecutionId executionId) {
        return new ExecutionId(executionId.getId() + 1, executionId.getAttempt(), executionId.getTotal() + 1);
    }

    public static ExecutionId nextAttempt(ExecutionId executionId) {
        return new ExecutionId(executionId.getId(), executionId.getAttempt() + 1, executionId.getTotal() + 1);
    }
}
