package com.netflix.titus.api.model.reference;

import java.util.Map;
import java.util.Objects;

import com.netflix.titus.api.model.Level;
import com.netflix.titus.api.model.Tier;

import static com.netflix.titus.common.util.CollectionsExt.newMapFrom;

public class TierReference extends Reference {

    private static final Map<Tier, com.netflix.titus.api.model.reference.TierReference> TIER_REFERENCES = newMapFrom(Tier.values(), com.netflix.titus.api.model.reference.TierReference::new);

    private final Tier tier;

    public TierReference(Tier tier) {
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
        com.netflix.titus.api.model.reference.TierReference that = (com.netflix.titus.api.model.reference.TierReference) o;
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

    public static TierReference getInstance(Tier tier) {
        return TIER_REFERENCES.get(tier);
    }
}
