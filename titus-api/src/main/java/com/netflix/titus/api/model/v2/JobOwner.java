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

package com.netflix.titus.api.model.v2;

import java.net.URL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

public class JobOwner {
    private final String name;
    private final String teamName;
    private final String description;
    private final String contactEmail;
    private final URL repo;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public JobOwner(@JsonProperty("name") String name, @JsonProperty("teamName") String teamName,
                    @JsonProperty("description") String description, @JsonProperty("contactEmail") String contactEmail,
                    @JsonProperty("repo") URL repo) {
        this.name = name;
        this.teamName = teamName;
        this.description = description;
        this.contactEmail = contactEmail;
        this.repo = repo;
    }

    public String getName() {
        return name;
    }

    public String getTeamName() {
        return teamName;
    }

    public String getDescription() {
        return description;
    }

    public String getContactEmail() {
        return contactEmail;
    }

    public URL getRepo() {
        return repo;
    }
}
