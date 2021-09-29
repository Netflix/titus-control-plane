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

package com.netflix.titus.ext.kube.clustermembership.connector.transport.fabric8io.crd;

import java.util.List;
import java.util.stream.Collectors;

import com.netflix.titus.api.clustermembership.model.ClusterMember;
import com.netflix.titus.api.clustermembership.model.ClusterMemberAddress;
import com.netflix.titus.api.clustermembership.model.ClusterMembershipRevision;
import io.fabric8.kubernetes.api.model.ObjectMeta;

public class Fabric8IOModelConverters {

    public static ClusterMembershipRevision<ClusterMember> toClusterMembershipRevision(FClusterMembershipSpec spec) {
        return ClusterMembershipRevision.<ClusterMember>newBuilder()
                .withCurrent(toClusterMember(spec.getCurrent()))
                .withCode(spec.getCode())
                .withMessage(spec.getMessage())
                .withTimestamp(spec.getTimestamp())
                .build();
    }

    public static ClusterMember toClusterMember(FClusterMember kubeClusterMember) {
        return ClusterMember.newBuilder()
                .withMemberId(kubeClusterMember.getMemberId())
                .withEnabled(kubeClusterMember.isEnabled())
                .withActive(kubeClusterMember.isActive())
                .withClusterMemberAddresses(toClusterMemberAddresses(kubeClusterMember.getClusterMemberAddresses()))
                .withLabels(kubeClusterMember.getLabels())
                .build();
    }

    public static List<ClusterMemberAddress> toClusterMemberAddresses(List<FClusterMemberAddress> kubeAddresses) {
        return kubeAddresses.stream().map(Fabric8IOModelConverters::toClusterMemberAddress).collect(Collectors.toList());
    }

    public static ClusterMemberAddress toClusterMemberAddress(FClusterMemberAddress kubeAddress) {
        return ClusterMemberAddress.newBuilder()
                .withIpAddress(kubeAddress.getIpAddress())
                .withPortNumber(kubeAddress.getPortNumber())
                .withProtocol(kubeAddress.getProtocol())
                .withSecure(kubeAddress.isSecure())
                .withDescription(kubeAddress.getDescription())
                .build();
    }

    public static FClusterMembership toCrdFClusterMembership(ClusterMembershipRevision<ClusterMember> revision) {
        FClusterMembership crd = new FClusterMembership();
        ObjectMeta metadata = new ObjectMeta();
        metadata.setName(revision.getCurrent().getMemberId());
        crd.setMetadata(metadata);
        crd.setSpec(toCrdClusterMembershipRevisionResource(revision));
        return crd;
    }

    public static FClusterMembershipSpec toCrdClusterMembershipRevisionResource(ClusterMembershipRevision<ClusterMember> revision) {
        FClusterMembershipSpec fClusterMembership = new FClusterMembershipSpec();
        fClusterMembership.setCurrent(toCrdClusterMember(revision.getCurrent()));
        fClusterMembership.setCode(revision.getCode());
        fClusterMembership.setMessage(revision.getMessage());
        fClusterMembership.setTimestamp(revision.getTimestamp());
        return fClusterMembership;
    }

    public static FClusterMember toCrdClusterMember(ClusterMember clusterMember) {
        FClusterMember fClusterMember = new FClusterMember();
        fClusterMember.setMemberId(clusterMember.getMemberId());
        fClusterMember.setEnabled(clusterMember.isEnabled());
        fClusterMember.setActive(clusterMember.isActive());
        fClusterMember.setLabels(clusterMember.getLabels());
        fClusterMember.setClusterMemberAddresses(toCrdClusterMemberAddresses(clusterMember.getClusterMemberAddresses()));
        return fClusterMember;
    }

    public static List<FClusterMemberAddress> toCrdClusterMemberAddresses(List<ClusterMemberAddress> addresses) {
        return addresses.stream().map(Fabric8IOModelConverters::toCrdClusterMemberAddress).collect(Collectors.toList());
    }

    public static FClusterMemberAddress toCrdClusterMemberAddress(ClusterMemberAddress address) {
        FClusterMemberAddress fClusterMemberAddress = new FClusterMemberAddress();
        fClusterMemberAddress.setIpAddress(address.getIpAddress());
        fClusterMemberAddress.setPortNumber(address.getPortNumber());
        fClusterMemberAddress.setProtocol(address.getProtocol());
        fClusterMemberAddress.setSecure(address.isSecure());
        fClusterMemberAddress.setDescription(address.getDescription());
        return fClusterMemberAddress;
    }
}
