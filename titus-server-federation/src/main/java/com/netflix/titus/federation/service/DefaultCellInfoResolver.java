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
package com.netflix.titus.federation.service;

import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import com.netflix.titus.federation.startup.TitusFederationConfiguration;


@Singleton
public class DefaultCellInfoResolver implements CellInfoResolver {
    private final TitusFederationConfiguration configuration;

    @Inject
    public DefaultCellInfoResolver(TitusFederationConfiguration configuration) {
        this.configuration = configuration;
    }

    /**
     * @throws IllegalStateException when no cells are configured
     */
    @Override
    public List<Cell> resolve() {
        String configuredCells = configuration.getCells();
        List<Cell> cells = CellInfoUtil.extractCellsFromCellSpecification(configuredCells);
        if (cells.isEmpty()) {
            throw new IllegalStateException("No valid cells are configured: " + configuredCells);
        }
        return cells;
    }

    @Override
    public Cell getDefault() {
        return resolve().get(0);
    }
}
