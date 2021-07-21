/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.ext.jooq.profile;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DatabaseProfiles {

    private static final DatabaseProfiles EMPTY = new DatabaseProfiles(Collections.emptyMap());

    private final Map<String, DatabaseProfile> profiles;

    @JsonCreator
    public DatabaseProfiles(@JsonProperty("profiles") Map<String, DatabaseProfile> profiles) {
        this.profiles = profiles;
    }

    public Map<String, DatabaseProfile> getProfiles() {
        return profiles;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseProfiles that = (DatabaseProfiles) o;
        return Objects.equals(profiles, that.profiles);
    }

    @Override
    public int hashCode() {
        return Objects.hash(profiles);
    }

    @Override
    public String toString() {
        return "DatabaseProfiles{" +
                "profiles=" + profiles +
                '}';
    }

    public static DatabaseProfiles empty() {
        return EMPTY;
    }
}
