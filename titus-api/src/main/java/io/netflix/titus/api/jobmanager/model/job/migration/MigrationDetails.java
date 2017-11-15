package io.netflix.titus.api.jobmanager.model.job.migration;

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
