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

package io.netflix.titus.master.config;

import java.util.List;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

import static java.util.Arrays.asList;

/**
 * Converts {@link MasterConfiguration} values into other data types.
 */
public class MasterConfigurationConverters {

    private static final Pattern COMMA_RE = Pattern.compile(",");

    public static List<String> getDefaultSecurityGroupList(MasterConfiguration config) {
        return toDefaultSecurityGroupList(config.getDefaultSecurityGroupsList());
    }

    public static List<String> toDefaultSecurityGroupList(String commaSeparatedList) {
        Preconditions.checkNotNull(commaSeparatedList, "the configured default security group is null");
        return asList(COMMA_RE.split(commaSeparatedList, 0));
    }
}
