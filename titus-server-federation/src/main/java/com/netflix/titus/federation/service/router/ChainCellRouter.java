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

import java.util.List;
import java.util.Optional;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.grpc.protogen.JobDescriptor;

public class ChainCellRouter implements CellRouter {

    private final List<CellRouter> cellRouters;

    public ChainCellRouter(List<CellRouter> cellRouters) {
        this.cellRouters = cellRouters;
    }

    @Override
    public Optional<Cell> routeKey(JobDescriptor jobDescriptor) {
        for (CellRouter cellRouter : cellRouters) {
            Optional<Cell> result = cellRouter.routeKey(jobDescriptor);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }
}
