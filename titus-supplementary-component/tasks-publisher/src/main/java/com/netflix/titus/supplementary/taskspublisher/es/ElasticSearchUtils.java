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
package com.netflix.titus.supplementary.taskspublisher.es;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import com.google.common.annotations.VisibleForTesting;

public class ElasticSearchUtils {
    public static final SimpleDateFormat DATE_FORMAT;

    static {
        DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static String buildEsIndexNameCurrent(String esIndexPrefix, SimpleDateFormat indexDateFormatSuffix) {
        return String.format("%s%s", esIndexPrefix, indexDateFormatSuffix.format(new Date()));
    }
}
