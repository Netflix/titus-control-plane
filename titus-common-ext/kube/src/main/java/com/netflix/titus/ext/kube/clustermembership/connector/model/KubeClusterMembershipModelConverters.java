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

package com.netflix.titus.ext.kube.clustermembership.connector.model;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import io.kubernetes.client.models.V1ObjectMeta;

public class KubeClusterMembershipModelConverters {

    public static final String CRD_GROUP = "clustermembership.titus.netflix";
    public static final String CRD_RESOURCE = "Members";
    public static final String CRD_VERSION = "v1alpha1";

    public static final String CRD_API_VERSION = CRD_GROUP + '/' + CRD_VERSION;

    public static final String CRD_PLURAL_MEMBERS = "members";

    public static KubeClusterMembershipRevisionResource toKubeClusterMembershipRevisionResource(String namespace,
                                                                                                ClusterMembershipRevision<ClusterMember> revision) {
        ClusterMember member = revision.getCurrent();

        return new KubeClusterMembershipRevisionResource()
                .apiVersion(CRD_API_VERSION)
                .kind(CRD_RESOURCE)
                .metadata(new V1ObjectMeta()
                        .namespace(namespace)
                        .name(member.getMemberId())
                        .resourceVersion(member.getLabels().get("resourceVersion"))
                )
                .spec(KubeClusterMembershipModelConverters.toKubeClusterMembershipRevision(revision));
    }

    public static KubeClusterMembershipRevision toKubeClusterMembershipRevision(ClusterMembershipRevision<ClusterMember> revision) {
        return new KubeClusterMembershipRevision()
                .current(toKubeClusterMember(revision.getCurrent()))
                .code(revision.getCode())
                .message(revision.getMessage())
                .timestamp(revision.getTimestamp());
    }

    public static ClusterMembershipRevision toClusterMembershipRevision(KubeClusterMembershipRevision kubeRevision) {
        return ClusterMembershipRevision.newBuilder()
                .withCurrent(toClusterMember(kubeRevision.getCurrent()))
                .withCode(kubeRevision.getCode())
                .withMessage(kubeRevision.getMessage())
                .withTimestamp(kubeRevision.getTimestamp())
                .build();
    }

    public static KubeClusterMember toKubeClusterMember(ClusterMember clusterMember) {
        return new KubeClusterMember()
                .memberId(clusterMember.getMemberId())
                .enabled(clusterMember.isEnabled())
                .active(clusterMember.isActive())
                .labels(clusterMember.getLabels())
                .clusterMemberAddress(toKubeClusterMemberAddresses(clusterMember.getClusterMemberAddresses()));
    }

    private static ClusterMember toClusterMember(KubeClusterMember kubeClusterMember) {
        return ClusterMember.newBuilder()
                .withMemberId(kubeClusterMember.getMemberId())
                .withEnabled(kubeClusterMember.isEnabled())
                .withActive(kubeClusterMember.isActive())
                .withClusterMemberAddresses(toClusterMemberAddresses(kubeClusterMember.getClusterMemberAddress()))
                .withLabels(kubeClusterMember.getLabels())
                .build();
    }

    public static List<KubeClusterMemberAddress> toKubeClusterMemberAddresses(List<ClusterMemberAddress> addresses) {
        return addresses.stream().map(KubeClusterMembershipModelConverters::toKubeClusterMemberAddress).collect(Collectors.toList());
    }

    public static List<ClusterMemberAddress> toClusterMemberAddresses(List<KubeClusterMemberAddress> kubeAddresses) {
        return kubeAddresses.stream().map(KubeClusterMembershipModelConverters::toClusterMemberAddress).collect(Collectors.toList());
    }

    public static KubeClusterMemberAddress toKubeClusterMemberAddress(ClusterMemberAddress address) {
        return new KubeClusterMemberAddress()
                .ipAddress(address.getIpAddress())
                .portNumber(address.getPortNumber())
                .protocol(address.getProtocol())
                .secure(address.isSecure())
                .description(address.getDescription());
    }

    public static ClusterMemberAddress toClusterMemberAddress(KubeClusterMemberAddress kubeAddress) {
        return ClusterMemberAddress.newBuilder()
                .withIpAddress(kubeAddress.getIpAddress())
                .withPortNumber(kubeAddress.getPortNumber())
                .withProtocol(kubeAddress.getProtocol())
                .withSecure(kubeAddress.isSecure())
                .withDescription(kubeAddress.getDescription())
                .build();
    }
}
