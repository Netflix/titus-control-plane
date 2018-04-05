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

package com.netflix.titus.runtime.endpoint.resolver;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServletRequest;

/**
 * Caller resolver that uses Eureka information.
 */
@Singleton
public class TitusHeaderHttpCallerIdResolver implements HttpCallerIdResolver {

    private static final String TITUS_HEADER_CALLER_HOST_ADDRESS = "X-Titus-CallerHostAddress";
    private static final String TITUS_HEADER_CALLER_ID = "X-Titus-CallerId";

    private final HostCallerIdResolver hostCallerIdResolver;

    @Inject
    public TitusHeaderHttpCallerIdResolver(HostCallerIdResolver hostCallerIdResolver) {
        this.hostCallerIdResolver = hostCallerIdResolver;
    }

    @Override
    public Optional<String> resolve(HttpServletRequest httpServletRequest) {
        String callerHost = Optional.ofNullable(httpServletRequest.getHeader(TITUS_HEADER_CALLER_HOST_ADDRESS)).orElse(httpServletRequest.getRemoteHost());
        String hostWithApp = hostCallerIdResolver.resolve(callerHost).map(app -> app + '(' + callerHost + ')').orElse(callerHost);
        String callerId = httpServletRequest.getHeader(TITUS_HEADER_CALLER_ID);
        return Optional.of(callerId == null ? hostWithApp : callerId + ';' + hostWithApp);
    }
}
