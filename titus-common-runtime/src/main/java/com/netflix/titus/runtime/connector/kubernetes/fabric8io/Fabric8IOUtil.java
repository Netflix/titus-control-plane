/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.runtime.connector.kubernetes.fabric8io;

import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public final class Fabric8IOUtil {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_INSTANT.withZone(ZoneId.of("Z"));

    private Fabric8IOUtil() {
    }

    /**
     * Kubernetes timestamps are returned as string values. For example "2022-01-20T15:31:20Z".
     */
    public static OffsetDateTime parseTimestamp(String timestamp) {
        return DATE_TIME_FORMATTER.parse(timestamp, ZonedDateTime::from).toOffsetDateTime();
    }

    public static String formatTimestamp(OffsetDateTime timestamp) {
        return DATE_TIME_FORMATTER.format(timestamp.toInstant());
    }
}
