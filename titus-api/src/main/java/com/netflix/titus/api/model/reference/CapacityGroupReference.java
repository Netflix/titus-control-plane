package com.netflix.titus.api.model.reference;

import java.time.Duration;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.netflix.titus.api.model.Level;

public class CapacityGroupReference extends Reference {

    private static final LoadingCache<String, CapacityGroupReference> CACHE = Caffeine.newBuilder()
            .expireAfterWrite(Duration.ofHours(1))
            .build(com.netflix.titus.api.model.reference.CapacityGroupReference::new);

    private final String name;

    private CapacityGroupReference(String name) {
        super(Level.CapacityGroup);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        com.netflix.titus.api.model.reference.CapacityGroupReference that = (com.netflix.titus.api.model.reference.CapacityGroupReference) o;
        return Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), name);
    }

    @Override
    public String toString() {
        return "CapacityGroupReference{" +
                "level=" + getLevel() +
                ", name='" + name + '\'' +
                "} " + super.toString();
    }

    public static CapacityGroupReference getInstance(String capacityGroupName) {
        return CACHE.get(capacityGroupName);
    }
}
