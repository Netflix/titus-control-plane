package com.netflix.titus.api.model;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;

import static com.netflix.titus.common.util.CollectionsExt.newMapFrom;

public abstract class Reference {

    private final Level level;

    private Reference(Level level) {
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
        return GlobalReference.INSTANCE;
    }

    public static Reference tier(Tier tier) {
        return TierReference.TIER_REFERENCES.get(tier);
    }

    public static Reference capacityGroup(String capacityGroupName) {
        return CapacityGroupReference.CACHE.get(capacityGroupName);
    }

    public static class GlobalReference extends Reference {
        private static final GlobalReference INSTANCE = new GlobalReference();

        private GlobalReference() {
            super(Level.Global);
        }

        @Override
        public String getName() {
            return "global";
        }
    }

    public static class TierReference extends Reference {

        private static final Map<Tier, TierReference> TIER_REFERENCES = newMapFrom(Tier.values(), TierReference::new);

        private final Tier tier;

        private TierReference(Tier tier) {
            super(Level.Tier);
            this.tier = tier;
        }

        public Tier getTier() {
            return tier;
        }

        @Override
        public String getName() {
            return tier.name();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TierReference that = (TierReference) o;
            return tier == that.tier;
        }

        @Override
        public int hashCode() {
            return Objects.hash(tier);
        }

        @Override
        public String toString() {
            return "TierReference{" +
                    "level=" + getLevel() +
                    ", tier=" + tier +
                    "} " + super.toString();
        }
    }

    public static class CapacityGroupReference extends Reference {

        private static final LoadingCache<String, Reference> CACHE = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofHours(1))
                .build(CapacityGroupReference::new);

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
            CapacityGroupReference that = (CapacityGroupReference) o;
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
    }
}
