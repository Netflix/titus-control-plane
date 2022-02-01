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

package com.netflix.titus.client.clustermembership.resolver;

import java.util.function.Function;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.common.util.StringExt;

public class NameClusterMemberVerifier implements ClusterMemberVerifier {

    private final String expectedStackName;
    private final Function<ClusterMember, String> clusterNameResolve;

    public NameClusterMemberVerifier(String expectedStackName,
                                     Function<ClusterMember, String> clusterNameResolve) {
        this.expectedStackName = StringExt.safeTrim(expectedStackName);
        this.clusterNameResolve = clusterNameResolve;
    }

    @Override
    public ClusterMemberVerifierResult verify(ClusterMember clusterMember) {
        if (expectedStackName.isEmpty()) {
            return ClusterMemberVerifierResult.valid();
        }
        String stackName = clusterNameResolve.apply(clusterMember);
        // Allow the null value for backward compatibility (old members data do not include their stack names).
        if (stackName == null || stackName.equals(expectedStackName)) {
            return ClusterMemberVerifierResult.valid();
        }
        return ClusterMemberVerifierResult.invalid(
                String.format("Invalid member stack: expected=%s, actual=%s", expectedStackName, stackName)
        );
    }

    public static ClusterMemberVerifier fromLabel(String expectedStackName, String labelName) {
        return new NameClusterMemberVerifier(
                expectedStackName,
                clusterMember -> clusterMember.getLabels().get(labelName)
        );
    }
}
