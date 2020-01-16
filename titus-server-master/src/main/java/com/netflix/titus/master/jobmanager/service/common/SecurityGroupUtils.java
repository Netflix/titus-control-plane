/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.master.jobmanager.service.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public final class SecurityGroupUtils {
    private static final String SECURITY_GROUP_DELIMITER = ":";

    public static String normalizeSecurityGroups(String securityGroups) {
        return normalizedSecurityGroupsIdentifier(Arrays.asList(securityGroups.split(SECURITY_GROUP_DELIMITER)));
    }

    static String normalizedSecurityGroupsIdentifier(List<String> securityGroups) {
        // make sure the security groups are sorted when loading two level resources
        List<String> sortedSecurityGroups = new ArrayList<>(securityGroups);
        Collections.sort(sortedSecurityGroups);
        return String.join(SECURITY_GROUP_DELIMITER, sortedSecurityGroups);
    }

}
