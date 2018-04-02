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
package io.netflix.titus.federation.service;

import com.netflix.titus.grpc.protogen.AddLoadBalancerRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersRequest;
import com.netflix.titus.grpc.protogen.GetAllLoadBalancersResult;
import com.netflix.titus.grpc.protogen.GetJobLoadBalancersResult;
import com.netflix.titus.grpc.protogen.JobId;
import com.netflix.titus.grpc.protogen.RemoveLoadBalancerRequest;
import io.grpc.Status;
import io.grpc.StatusException;
import io.netflix.titus.runtime.loadbalancer.LoadBalancerCursors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Completable;
import rx.Observable;

public class AggregatingLoadbalancerService implements LoadBalancerService {
    private static final Logger logger = LoggerFactory.getLogger(AggregatingLoadbalancerService.class);

    @Override
    public Observable<GetAllLoadBalancersResult> getAllLoadBalancers(GetAllLoadBalancersRequest request) {
        return Observable.error(notImplemented("addLoadBalancer"));
    }

    @Override
    public Observable<GetJobLoadBalancersResult> getLoadBalancers(JobId jobId) {
        return Observable.error(notImplemented("getLoadBalancers"));
    }

    @Override
    public Completable addLoadBalancer(AddLoadBalancerRequest request) {
        return Completable.error(notImplemented("addLoadBalancer"));
    }

    @Override
    public Completable removeLoadBalancer(RemoveLoadBalancerRequest removeLoadBalancerRequest) {
        return Completable.error(notImplemented("removeLoadBalancer"));
    }


    private static StatusException notImplemented(String operation) {
        return Status.UNIMPLEMENTED.withDescription(operation + " is not implemented").asException();
    }
}
