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
