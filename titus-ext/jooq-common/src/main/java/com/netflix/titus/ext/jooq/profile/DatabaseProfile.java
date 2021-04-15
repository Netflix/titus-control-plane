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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Database configuration.
 */
public class DatabaseProfile {

    private final String name;
    private final String databaseUrl;
    private final String user;
    private final String password;

    @JsonCreator
    public DatabaseProfile(@JsonProperty("name") String name,
                           @JsonProperty("databaseUrl") String databaseUrl,
                           @JsonProperty("user") String user,
                           @JsonProperty("password") String password) {
        this.name = name;
        this.databaseUrl = databaseUrl;
        this.user = user;
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public String getDatabaseUrl() {
        return databaseUrl;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DatabaseProfile that = (DatabaseProfile) o;
        return Objects.equals(name, that.name) && Objects.equals(databaseUrl, that.databaseUrl) && Objects.equals(user, that.user) && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, databaseUrl, user, password);
    }

    @Override
    public String toString() {
        return "DatabaseProfile{" +
                "name='" + name + '\'' +
                ", databaseUrl='" + databaseUrl + '\'' +
                ", user='" + user + '\'' +
                ", password='" + password + '\'' +
                '}';
    }
}
