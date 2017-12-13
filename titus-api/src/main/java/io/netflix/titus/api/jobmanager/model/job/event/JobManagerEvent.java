package io.netflix.titus.api.jobmanager.model.job.event;

import java.util.Optional;

public abstract class JobManagerEvent<TYPE> {

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
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        JobManagerEvent<?> that = (JobManagerEvent<?>) o;

        if (current != null ? !current.equals(that.current) : that.current != null) {
            return false;
        }
        return previous != null ? previous.equals(that.previous) : that.previous == null;
    }

    @Override
    public int hashCode() {
        int result = current != null ? current.hashCode() : 0;
        result = 31 * result + (previous != null ? previous.hashCode() : 0);
        return result;
    }
}
