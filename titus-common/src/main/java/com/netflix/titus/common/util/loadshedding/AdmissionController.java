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

package com.netflix.titus.common.util.loadshedding;

import java.util.function.Function;

/**
 * {@link AdmissionController} monitors incoming request rate, and decides if an arriving request
 * should be allowed or discarded. All implementations must be thread safe and support concurrent invocations.
 */
public interface AdmissionController extends Function<AdmissionControllerRequest, AdmissionControllerResponse> {
}
