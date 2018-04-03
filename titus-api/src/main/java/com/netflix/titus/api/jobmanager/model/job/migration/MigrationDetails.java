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

package com.netflix.titus.api.jobmanager.model.job.migration;

public class MigrationDetails {
    private final boolean needsMigration;
    private final long deadline;

    public MigrationDetails(boolean needsMigration, long deadline) {
        this.needsMigration = needsMigration;
        this.deadline = deadline;
    }

    public boolean isNeedsMigration() {
        return needsMigration;
    }

    public long getDeadline() {
        return deadline;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MigrationDetails that = (MigrationDetails) o;

        if (needsMigration != that.needsMigration) {
            return false;
        }
        return deadline == that.deadline;
    }

    @Override
    public int hashCode() {
        int result = (needsMigration ? 1 : 0);
        result = 31 * result + (int) (deadline ^ (deadline >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "MigrationDetails{" +
                "needsMigration=" + needsMigration +
                ", deadline=" + deadline +
                '}';
    }

    public static MigrationDetailsBuilder newBuilder() {
        return new MigrationDetailsBuilder();
    }

    public static final class MigrationDetailsBuilder {
        private boolean needsMigration;
        private long deadline;

        private MigrationDetailsBuilder() {
        }

        public MigrationDetailsBuilder withNeedsMigration(boolean needsMigration) {
            this.needsMigration = needsMigration;
            return this;
        }

        public MigrationDetailsBuilder withDeadline(long deadline) {
            this.deadline = deadline;
            return this;
        }

        public MigrationDetailsBuilder but() {
            return MigrationDetails.newBuilder().withNeedsMigration(needsMigration).withDeadline(deadline);
        }

        public MigrationDetails build() {
            return new MigrationDetails(needsMigration, deadline);
        }
    }
}
