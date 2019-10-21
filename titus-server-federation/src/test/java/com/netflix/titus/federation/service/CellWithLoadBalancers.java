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
package com.netflix.titus.federation.service;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.protobuf.Empty;
import com.netflix.titus.api.loadbalancer.model.JobLoadBalancer;
import com.netflix.titus.api.model.PaginationUtil;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.LoadBalancerId;
import com.netflix.titus.grpc.protogen.LoadBalancerServiceGrpc;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import com.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toGrpcPagination;
import static com.netflix.titus.runtime.endpoint.v3.grpc.GrpcJobQueryModelConverters.toPage;

class CellWithLoadBalancers extends LoadBalancerServiceGrpc.LoadBalancerServiceImplBase {
    private static final Logger logger = LoggerFactory.getLogger(CellWithLoadBalancers.class);
    private List<JobLoadBalancer> jobLoadBalancerList;

    CellWithLoadBalancers(List<JobLoadBalancer> jobLoadBalancerList) {
        this.jobLoadBalancerList = jobLoadBalancerList;
    }

    @Override
    public void getJobLoadBalancers(JobId request, StreamObserver<GetJobLoadBalancersResult> responseObserver) {
        responseObserver.onNext(buildJobLoadBalancersResult(jobLoadBalancerList, request.getId()));
        responseObserver.onCompleted();
    }

    @Override
    public void getAllLoadBalancers(GetAllLoadBalancersRequest request, StreamObserver<GetAllLoadBalancersResult> responseObserver) {

        Pair<List<JobLoadBalancer>, com.netflix.titus.api.model.Pagination> page = PaginationUtil.takePageWithCursor(
                toPage(request.getPage()),
                jobLoadBalancerList,
                LoadBalancerCursors.loadBalancerComparator(),
                LoadBalancerCursors::loadBalancerIndexOf,
                LoadBalancerCursors::newCursorFrom
        );

        final List<JobLoadBalancer> jobLoadBalancersList = page.getLeft();
        final Set<String> jobIds = jobLoadBalancersList.stream().map(jobLoadBalancer -> jobLoadBalancer.getJobId()).collect(Collectors.toSet());

        final GetAllLoadBalancersResult.Builder allResultsBuilder = GetAllLoadBalancersResult.newBuilder();
        final List<GetJobLoadBalancersResult> getJobLoadBalancersResults = jobIds.stream()
                .map(jid -> buildJobLoadBalancersResult(jobLoadBalancersList, jid))
                .collect(Collectors.toList());

        allResultsBuilder.setPagination(toGrpcPagination(page.getRight())).addAllJobLoadBalancers(getJobLoadBalancersResults);
        responseObserver.onNext(allResultsBuilder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void addLoadBalancer(AddLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        jobLoadBalancerList.add(new JobLoadBalancer(request.getJobId(), request.getLoadBalancerId().getId()));
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void removeLoadBalancer(RemoveLoadBalancerRequest request, StreamObserver<Empty> responseObserver) {
        final String loadBalancerToDelete = request.getLoadBalancerId().getId();
        jobLoadBalancerList = jobLoadBalancerList.stream()
                .filter(jobLoadBalancer -> !jobLoadBalancer.getLoadBalancerId().equals(loadBalancerToDelete)).collect(Collectors.toList());
        responseObserver.onNext(Empty.getDefaultInstance());
        responseObserver.onCompleted();
    }

    private GetJobLoadBalancersResult buildJobLoadBalancersResult(List<JobLoadBalancer> lbList, String jobId) {
        final List<LoadBalancerId> loadBalancerIdsForJob = lbList.stream().filter(jobLoadBalancer -> jobLoadBalancer.getJobId().equals(jobId))
                .map(jobLoadBalancer -> LoadBalancerId.newBuilder().setId(jobLoadBalancer.getLoadBalancerId()).build())
                .collect(Collectors.toList());
        return GetJobLoadBalancersResult.newBuilder().setJobId(jobId).addAllLoadBalancers(loadBalancerIdsForJob).build();
    }
}
