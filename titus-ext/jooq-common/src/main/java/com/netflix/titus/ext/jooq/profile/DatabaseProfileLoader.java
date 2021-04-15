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

import java.io.File;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public final class DatabaseProfileLoader {

    public static final String LOCAL_CONFIGURATION_FILE = ".titus-jooq.yaml";

    /**
     * Load {@link DatabaseProfile}s from the home folder.
     */
    public static DatabaseProfiles loadLocal() {
        File file = new File(System.getenv("HOME"), LOCAL_CONFIGURATION_FILE);
        if (!file.exists()) {
            return DatabaseProfiles.empty();
        }
        return loadFromFile(file);
    }

    public static DatabaseProfiles loadFromFile(File file) {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            return mapper.readValue(file, DatabaseProfiles.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot load local database setup from " + file, e);
        }
    }
}
