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

package com.netflix.titus.federation.service.router;

import java.util.Optional;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.grpc.protogen.JobDescriptor;

/**
 * CellRouter determines which cell to route a request to amongst a set of Cells.
 */
public interface CellRouter {
    /**
     * routeKey is a blocking call that returns the Cell to use based on the {@link JobDescriptor}.
     * The key should correspond to what the routing implementation expects, e.g., Job IDs
     * or Capacity Group names.
     *
     * @param jobDescriptor
     * @return Cell
     */
    Optional<Cell> routeKey(JobDescriptor jobDescriptor);
}
