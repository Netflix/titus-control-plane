/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.master.endpoint.v2.rest;

import com.netflix.archaius.api.annotations.Configuration;
import com.netflix.archaius.api.annotations.DefaultValue;
import io.netflix.titus.runtime.endpoint.common.rest.RestServerConfiguration;

@Configuration(prefix = "titus.master.jaxrs")
public interface RestConfig extends RestServerConfiguration {

    /**
     * If set to true, multi-job query operation is restricted to live data only, even if the client explicitly requests it.
     * If set to false, archived data are included, which requires storage access, and may cause performance issue.
     * This option does not affect direct job id/task id queries.
     */
    @DefaultValue("true")
    boolean isArchiveDataQueryRestricted();
}
