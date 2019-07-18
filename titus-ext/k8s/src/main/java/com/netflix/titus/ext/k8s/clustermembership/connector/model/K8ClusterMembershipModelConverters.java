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

package com.netflix.titus.ext.k8s.clustermembership.connector.model;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMemberState;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import io.kubernetes.client.models.V1ObjectMeta;

public class K8ClusterMembershipModelConverters {

    public static final String CRD_GROUP = "clustermembership.titus.netflix";
    public static final String CRD_RESOURCE = "Members";
    public static final String CRD_VERSION = "v1alpha1";

    public static final String CRD_API_VERSION = CRD_GROUP + '/' + CRD_VERSION;

    public static final String CRD_PLURAL_MEMBERS = "members";

    public static K8ClusterMembershipRevisionResource toK8ClusterMembershipRevisionResource(String namespace,
                                                                                            ClusterMembershipRevision<ClusterMember> revision) {
        ClusterMember member = revision.getCurrent();

        return new K8ClusterMembershipRevisionResource()
                .apiVersion(CRD_API_VERSION)
                .kind(CRD_RESOURCE)
                .metadata(new V1ObjectMeta()
                        .namespace(namespace)
                        .name(member.getMemberId())
                        .resourceVersion(member.getLabels().get("resourceVersion"))
                )
                .spec(K8ClusterMembershipModelConverters.toK8ClusterMembershipRevision(revision));
    }

    public static K8ClusterMembershipRevision toK8ClusterMembershipRevision(ClusterMembershipRevision<ClusterMember> revision) {
        return new K8ClusterMembershipRevision()
                .current(toK8ClusterMember(revision.getCurrent()))
                .code(revision.getCode())
                .message(revision.getMessage())
                .timestamp(revision.getTimestamp());
    }

    public static ClusterMembershipRevision toClusterMembershipRevision(K8ClusterMembershipRevision k8Revision) {
        return ClusterMembershipRevision.newBuilder()
                .withCurrent(toClusterMember(k8Revision.getCurrent()))
                .withCode(k8Revision.getCode())
                .withMessage(k8Revision.getMessage())
                .withTimestamp(k8Revision.getTimestamp())
                .build();
    }

    public static K8ClusterMember toK8ClusterMember(ClusterMember clusterMember) {
        return new K8ClusterMember()
                .memberId(clusterMember.getMemberId())
                .state(clusterMember.getState().name())
                .enabled(clusterMember.isEnabled())
                .labels(clusterMember.getLabels())
                .clusterMemberAddress(toK8ClusterMemberAddresses(clusterMember.getClusterMemberAddresses()));
    }

    private static ClusterMember toClusterMember(K8ClusterMember k8ClusterMember) {
        return ClusterMember.newBuilder()
                .withMemberId(k8ClusterMember.getMemberId())
                .withEnabled(k8ClusterMember.isEnabled())
                .withState(ClusterMemberState.valueOf(k8ClusterMember.getState()))
                .withClusterMemberAddresses(toClusterMemberAddresses(k8ClusterMember.getClusterMemberAddress()))
                .withLabels(k8ClusterMember.getLabels())
                .build();
    }

    public static List<K8ClusterMemberAddress> toK8ClusterMemberAddresses(List<ClusterMemberAddress> addresses) {
        return addresses.stream().map(K8ClusterMembershipModelConverters::toK8ClusterMemberAddress).collect(Collectors.toList());
    }

    public static List<ClusterMemberAddress> toClusterMemberAddresses(List<K8ClusterMemberAddress> k8Addresses) {
        return k8Addresses.stream().map(K8ClusterMembershipModelConverters::toClusterMemberAddress).collect(Collectors.toList());
    }

    public static K8ClusterMemberAddress toK8ClusterMemberAddress(ClusterMemberAddress address) {
        return new K8ClusterMemberAddress()
                .ipAddress(address.getIpAddress())
                .portNumber(address.getPortNumber())
                .protocol(address.getProtocol())
                .secure(address.isSecure())
                .description(address.getDescription());
    }

    public static ClusterMemberAddress toClusterMemberAddress(K8ClusterMemberAddress k8Address) {
        return ClusterMemberAddress.newBuilder()
                .withIpAddress(k8Address.getIpAddress())
                .withPortNumber(k8Address.getPortNumber())
                .withProtocol(k8Address.getProtocol())
                .withSecure(k8Address.isSecure())
                .withDescription(k8Address.getDescription())
                .build();
    }
}
