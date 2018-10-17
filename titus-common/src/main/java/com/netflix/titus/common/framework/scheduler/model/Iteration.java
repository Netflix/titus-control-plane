package com.netflix.titus.common.framework.scheduler.model;

import java.util.Objects;

public class Iteration {

    private static final Iteration INITIAL = new Iteration(1, 1, 1);

    private final int id;
    private final int attempt;
    private final int total;

    public Iteration(int id, int attempt, int total) {
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
        Iteration that = (Iteration) o;
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

    public static Iteration initial() {
        return INITIAL;
    }

    public static Iteration nextIteration(Iteration iteration) {
        return new Iteration(iteration.getId() + 1, iteration.getAttempt(), iteration.getTotal() + 1);
    }

    public static Iteration nextAttempt(Iteration iteration) {
        return new Iteration(iteration.getId(), iteration.getAttempt() + 1, iteration.getTotal() + 1);
    }
}
