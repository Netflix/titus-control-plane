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

package com.netflix.titus.common.runtime.internal;

import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.SystemLogService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class AggregatingSystemLogService implements SystemLogService {

    private static final Logger logger = LoggerFactory.getLogger(AggregatingSystemLogService.class);

    private final Set<SystemLogService> delegates;

    @Inject
    public AggregatingSystemLogService(Set<SystemLogService> delegates) {
        this.delegates = delegates;
    }

    @Override
    public boolean submit(SystemLogEvent event) {
        boolean result = true;
        for (SystemLogService delegate : delegates) {
            try {
                result = result && delegate.submit(event);
            } catch (Exception e) {
                logger.warn("Logging error in delegate {}: {}", delegate.getClass().getSimpleName(), e.getMessage());
                result = false;
            }
        }
        return result;
    }
}
