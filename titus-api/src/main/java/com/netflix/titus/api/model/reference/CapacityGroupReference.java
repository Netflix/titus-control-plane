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
