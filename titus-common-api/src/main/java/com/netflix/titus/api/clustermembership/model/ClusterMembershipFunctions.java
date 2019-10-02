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

package com.netflix.titus.api.clustermembership.model;

import java.util.List;

import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.common.util.StringExt;

/**
 * A collection of common functions operating on {@link ClusterMembershipRevision} and related entities.
 */
public final class ClusterMembershipFunctions {

    private ClusterMembershipFunctions() {
    }

    public static boolean hasIpAddress(ClusterMember clusterMember, String ipAddress) {
        if (StringExt.isEmpty(ipAddress)) {
            return false;
        }
        List<ClusterMemberAddress> addresses = clusterMember.getClusterMemberAddresses();
        if (CollectionsExt.isNullOrEmpty(addresses)) {
            return false;
        }
        for (ClusterMemberAddress address : addresses) {
            if (ipAddress.equals(address.getIpAddress())) {
                return true;
            }
        }
        return false;
    }

    public static String toStringUri(ClusterMemberAddress address) {
        return address.getProtocol() + "://" + address.getIpAddress() + ':' + address.getPortNumber();
    }
}
