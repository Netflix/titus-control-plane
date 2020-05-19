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

package com.netflix.titus.runtime.endpoint.metadata.spring;

import java.util.Collection;

import com.netflix.titus.api.model.callmetadata.CallMetadata;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;

/**
 * A decorator for the {@link Authentication} object, which adds resolved request {@link CallMetadata}.
 */
public class CallMetadataAuthentication implements Authentication {

    private final Authentication delegate;
    private final CallMetadata callMetadata;

    public CallMetadataAuthentication(CallMetadata callMetadata, Authentication delegate) {
        this.delegate = delegate;
        this.callMetadata = callMetadata;
    }

    public CallMetadata getCallMetadata() {
        return callMetadata;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public Collection<? extends GrantedAuthority> getAuthorities() {
        return delegate.getAuthorities();
    }

    @Override
    public Object getCredentials() {
        return delegate.getCredentials();
    }

    @Override
    public Object getDetails() {
        return delegate.getDetails();
    }

    @Override
    public Object getPrincipal() {
        return delegate.getPrincipal();
    }

    @Override
    public boolean isAuthenticated() {
        return delegate.isAuthenticated();
    }

    @Override
    public void setAuthenticated(boolean isAuthenticated) throws IllegalArgumentException {
        delegate.setAuthenticated(isAuthenticated);
    }
}
