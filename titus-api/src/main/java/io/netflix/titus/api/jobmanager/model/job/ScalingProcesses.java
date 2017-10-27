package io.netflix.titus.api.jobmanager.model.job;

public class ScalingProcesses {
    private final boolean disableIncreaseDesired;

    private final boolean disableDecreaseDesired;

    public ScalingProcesses(boolean disableIncreaseDesired, boolean disableDecreaseDesired) {
        this.disableIncreaseDesired = disableIncreaseDesired;
        this.disableDecreaseDesired = disableDecreaseDesired;
    }

    public boolean isDisableIncreaseDesired() {
        return disableIncreaseDesired;
    }

    public boolean isDisableDecreaseDesired() {
        return disableDecreaseDesired;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScalingProcesses that = (ScalingProcesses) o;

        if (disableIncreaseDesired != that.disableIncreaseDesired) return false;
        return disableDecreaseDesired == that.disableDecreaseDesired;
    }

    @Override
    public int hashCode() {
        int result = (disableIncreaseDesired ? 1 : 0);
        result = 31 * result + (disableDecreaseDesired ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return "ScalingProcesses{" +
                "disableIncreaseDesired=" + disableIncreaseDesired +
                ", disableDecreaseDesired=" + disableDecreaseDesired +
                '}';
    }

    public static ScalingProcesses.Builder newBuilder() {
        return new ScalingProcesses.Builder();
    }



    public static final class Builder {
        private boolean disableIncreaseDesired;
        private boolean disableDecreaseDesired;

        private Builder() {
        }

        public static Builder aScalingProcesses() {
            return new Builder();
        }

        public Builder withDisableIncreaseDesired(boolean disableIncreaseDesired) {
            this.disableIncreaseDesired = disableIncreaseDesired;
            return this;
        }

        public Builder withDisableDecreaseDesired(boolean disableDecreaseDesired) {
            this.disableDecreaseDesired = disableDecreaseDesired;
            return this;
        }

        public ScalingProcesses build() {
            return new ScalingProcesses(disableIncreaseDesired, disableDecreaseDesired);
        }
    }
}
