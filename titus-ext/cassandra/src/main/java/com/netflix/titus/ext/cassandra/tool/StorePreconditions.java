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

package com.netflix.titus.ext.cassandra.tool;

import java.util.regex.Pattern;

import com.google.common.base.Preconditions;
import com.netflix.titus.common.util.StringExt;

/**
 * A collection of invariant predicates/enforcers.
 */
public final class StorePreconditions {

    private static Pattern[] DEV_STACK_PATTERNS = new Pattern[]{
            Pattern.compile(".*dev.*"),
            Pattern.compile(".*(test|Testing).*"),
            Pattern.compile(".*backup.*"),
            Pattern.compile(".*BACKUP.*"),
    };

    private StorePreconditions() {
    }

    /**
     * Check if keyspace belongs to development or test stack. Certain commands are destructive, and should be never
     * allowed on production stacks.
     */
    public static boolean isDevOrBackupStack(String keySpaceName) {
        Preconditions.checkArgument(StringExt.isNotEmpty(keySpaceName), "Expected keyspace name not null");

        for (Pattern p : DEV_STACK_PATTERNS) {
            if (p.matcher(keySpaceName).matches()) {
                return true;
            }
        }
        return false;
    }
}
