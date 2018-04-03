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

package com.netflix.titus.master.store.sanitizer;

import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.model.ApplicationSLA;
import com.netflix.titus.common.model.sanitizer.EntitySanitizer;
import com.netflix.titus.master.store.ApplicationSlaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

@Singleton
public class ApplicationSlaStoreSanitizer implements ApplicationSlaStore {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationSlaStoreSanitizer.class);

    private final ApplicationSlaStore delegate;
    private final EntitySanitizer coreModelSanitizers;

    @Inject
    public ApplicationSlaStoreSanitizer(ApplicationSlaStore delegate, EntitySanitizer coreModelSanitizers) {
        this.delegate = delegate;
        this.coreModelSanitizers = coreModelSanitizers;
    }

    @Override
    public Observable<Void> create(ApplicationSLA applicationSLA) {
        return delegate.create(applicationSLA);
    }

    @Override
    public Observable<ApplicationSLA> findAll() {
        return delegate.findAll().map(entity -> sanitize(entity).orElse(entity));
    }

    @Override
    public Observable<ApplicationSLA> findByName(String applicationName) {
        return delegate.findByName(applicationName).map(entity -> sanitize(entity).orElse(entity));
    }

    @Override
    public Observable<Void> remove(String applicationName) {
        return delegate.remove(applicationName);
    }

    private Optional<ApplicationSLA> sanitize(ApplicationSLA entity) {
        Optional<ApplicationSLA> sanitizedOpt = coreModelSanitizers.sanitize(entity);
        sanitizedOpt.ifPresent(sanitized ->
                logger.info("Sanitized entity: original={}, sanitized={}", entity, sanitized)
        );
        return sanitizedOpt;
    }
}
