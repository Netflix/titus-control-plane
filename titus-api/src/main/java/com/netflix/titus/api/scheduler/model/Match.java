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

package com.netflix.titus.api.scheduler.model;

import java.util.Objects;
import javax.validation.constraints.NotNull;

/**
 * Operator that applies a match expression if the select expression is true.
 */
public class Match extends Operator {

    @NotNull
    private final String selectExpression;

    @NotNull
    private final String matchExpression;

    public Match(String selectExpression, String matchExpression) {
        this.selectExpression = selectExpression;
        this.matchExpression = matchExpression;
    }

    public String getSelectExpression() {
        return selectExpression;
    }

    public String getMatchExpression() {
        return matchExpression;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Match match = (Match) o;
        return Objects.equals(selectExpression, match.selectExpression) &&
                Objects.equals(matchExpression, match.matchExpression);
    }

    @Override
    public int hashCode() {

        return Objects.hash(selectExpression, matchExpression);
    }

    @Override
    public String toString() {
        return "Match{" +
                "selectExpression='" + selectExpression + '\'' +
                ", matchExpression='" + matchExpression + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {
        private String selectExpression;
        private String matchExpression;

        private Builder() {
        }

        public Builder withSelectExpression(String selectExpression) {
            this.selectExpression = selectExpression;
            return this;
        }

        public Builder withMatchExpression(String matchExpression) {
            this.matchExpression = matchExpression;
            return this;
        }

        public Builder but() {
            return newBuilder().withSelectExpression(selectExpression).withMatchExpression(matchExpression);
        }

        public Match build() {
            return new Match(selectExpression, matchExpression);
        }
    }
}
