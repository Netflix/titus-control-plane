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

package io.netflix.titus.api.endpoint.v2.rest.representation;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;

public final class TypeReferences {

    public static final TypeReference<List<TitusJobInfo>> TITUS_JOB_INFO_LIST_TREF =
            new TypeReference<List<TitusJobInfo>>() {
            };

    public static final TypeReference<List<ApplicationSlaRepresentation>> APPLICATION_SLA_REP_LIST_TREF =
            new TypeReference<List<ApplicationSlaRepresentation>>() {
            };

    private TypeReferences() {
    }
}
