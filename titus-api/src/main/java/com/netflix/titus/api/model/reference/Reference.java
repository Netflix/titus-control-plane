package com.netflix.titus.api.model.reference;

import java.util.Objects;

import com.netflix.titus.api.model.Level;
import com.netflix.titus.api.model.Tier;

public abstract class Reference {

    private final Level level;

    protected Reference(Level level) {
        this.level = level;
    }

    public Level getLevel() {
        return level;
    }

    public abstract String getName();

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Reference reference = (Reference) o;
        return level == reference.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(level);
    }

    public static Reference global() {
        return GlobalReference.getInstance();
    }

    public static Reference tier(Tier tier) {
        return TierReference.getInstance(tier);
    }

    public static Reference capacityGroup(String capacityGroupName) {
        return CapacityGroupReference.getInstance(capacityGroupName);
    }
}
