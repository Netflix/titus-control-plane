/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.metadata;

import java.util.Map;
import java.util.Objects;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.CollectionsExt;

public class Caller {

    private final String id;
    private final CallerType callerType;
    private final Map<String, String> context;

    public Caller(String id, CallerType callerType, Map<String, String> context) {
        this.id = id;
        this.callerType = callerType;
        this.context = context;
    }

    public String getId() {
        return id;
    }

    public CallerType getCallerType() {
        return callerType;
    }

    public Map<String, String> getContext() {
        return context;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Caller caller = (Caller) o;
        return Objects.equals(id, caller.id) &&
                callerType == caller.callerType &&
                Objects.equals(context, caller.context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, callerType, context);
    }

    @Override
    public String toString() {
        return "Caller{" +
                "id='" + id + '\'' +
                ", callerType=" + callerType +
                ", context=" + context +
                '}';
    }

    public Builder toBuilder() {
        return newBuilder().withId(id).withCallerType(callerType).withContext(context);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String id;
        private CallerType callerType;
        private Map<String, String> context;

        private Builder() {
        }

        public Builder withId(String id) {
            this.id = id;
            return this;
        }

        public Builder withCallerType(CallerType callerType) {
            this.callerType = callerType;
            return this;
        }

        public Builder withContext(Map<String, String> context) {
            this.context = context;
            return this;
        }

        public Builder but() {
            return newBuilder().withId(id).withCallerType(callerType).withContext(context);
        }

        public Caller build() {
            Preconditions.checkNotNull(id, "Caller id is null");
            Preconditions.checkNotNull(callerType, "Caller type is null");

            return new Caller(id, callerType, CollectionsExt.nonNull(context));
        }
    }
}
