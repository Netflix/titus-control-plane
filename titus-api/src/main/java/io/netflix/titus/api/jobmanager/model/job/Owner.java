/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.api.jobmanager.model.job;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

/**
 */
public class Owner {

    @NotNull(message = "'teamEmail' missing")
    @Pattern(regexp = ".*@.*", message = "Invalid email address")
    private final String teamEmail;

    public Owner(String teamEmail) {
        this.teamEmail = teamEmail;
    }

    public String getTeamEmail() {
        return teamEmail;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Owner owner = (Owner) o;

        return teamEmail != null ? teamEmail.equals(owner.teamEmail) : owner.teamEmail == null;
    }

    @Override
    public int hashCode() {
        return teamEmail != null ? teamEmail.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "Owner{" +
                "teamEmail='" + teamEmail + '\'' +
                '}';
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Builder newBuilder(Owner owner) {
        return new Builder()
                .withTeamEmail(owner.getTeamEmail());
    }

    public static final class Builder {
        private String teamEmail;

        private Builder() {
        }

        public Builder withTeamEmail(String teamEmail) {
            this.teamEmail = teamEmail;
            return this;
        }

        public Owner build() {
            Owner owner = new Owner(teamEmail);
            return owner;
        }
    }
}
