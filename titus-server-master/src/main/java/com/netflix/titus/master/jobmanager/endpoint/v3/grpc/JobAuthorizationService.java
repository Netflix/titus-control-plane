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

package com.netflix.titus.master.jobmanager.endpoint.v3.grpc;

import java.util.Map;
import java.util.function.Predicate;

import com.netflix.titus.api.jobmanager.model.job.Job;
import com.netflix.titus.common.util.StringExt;
import com.netflix.titus.runtime.endpoint.authorization.AuthorizationService;
import com.netflix.titus.runtime.endpoint.authorization.AuthorizationStatus;
import com.netflix.titus.api.jobmanager.model.CallMetadata;
import reactor.core.publisher.Mono;


public abstract class JobAuthorizationService implements AuthorizationService {

    private static final String SECURITY_ATTRIBUTE_SECURITY_DOMAIN = "titus.securityDomain";

    private final Predicate<String> callerIdPredicate;

    protected JobAuthorizationService(Predicate<String> callerIdPredicate) {
        this.callerIdPredicate = callerIdPredicate;
    }

    @Override
    public <T> Mono<AuthorizationStatus> authorize(CallMetadata callMetadata, T object) {
        if (object instanceof Job) {
            return authorizeJob(callMetadata, (Job<?>) object);
        }
        return Mono.just(AuthorizationStatus.success("Access granted for non-job object: objectType=" + object.getClass()));
    }

    protected abstract Mono<AuthorizationStatus> authorize(String originalCallerId, String securityDomainId, Job<?> job);

    private Mono<AuthorizationStatus> authorizeJob(CallMetadata callMetadata, Job<?> job) {
        String originalCallerId = callMetadata.getCallerId();
        if (StringExt.isEmpty(originalCallerId)) {
            return Mono.just(AuthorizationStatus.success(
                    String.format("Request caller id missing; granting access to an identified user: jobId=%s, callMedata=%s", job.getId(), callMetadata)
            ));
        }

        if (!callerIdPredicate.test(originalCallerId)) {
            return Mono.just(AuthorizationStatus.success("User not white listed for authorization; granted by default: callerId=" + originalCallerId));
        }

        String securityDomainId = buildSecurityDomainId(job);

        return authorize(originalCallerId, securityDomainId, job);
    }

    private String buildSecurityDomainId(Job<?> job) {
        Map<String, String> securityAttributes = job.getJobDescriptor().getContainer().getSecurityProfile().getAttributes();
        String securityDomain = securityAttributes.get(SECURITY_ATTRIBUTE_SECURITY_DOMAIN);
        return StringExt.isNotEmpty(securityDomain) ? securityDomain : job.getJobDescriptor().getApplicationName();
    }
}
