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

package com.netflix.titus.federation.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.api.federation.model.Cell;
import org.springframework.web.reactive.function.client.WebClient;

@Singleton
public class DefaultCellWebClientConnector implements CellWebClientConnector {

    private final Map<Cell, WebClient> webClientsByCell;

    @Inject
    public DefaultCellWebClientConnector(CellInfoResolver cellInfoResolver,
                                         WebClientFactory cellWebClientFactory) {
        this.webClientsByCell = buildWebClients(cellInfoResolver, cellWebClientFactory);
    }

    @Override
    public Map<Cell, WebClient> getWebClients() {
        return webClientsByCell;
    }

    @Override
    public Optional<WebClient> getWebClientForCell(Cell cell) {
        return Optional.ofNullable(webClientsByCell.get(cell));
    }

    private Map<Cell, WebClient> buildWebClients(CellInfoResolver cellInfoResolver,
                                                 WebClientFactory cellWebClientFactory) {
        Set<Cell> resolvedCells = new HashSet<>(cellInfoResolver.resolve());

        Map<Cell, WebClient> webClientMap = new HashMap<>();
        resolvedCells.forEach(cell -> webClientMap.put(cell, cellWebClientFactory.apply(cell)));

        return Collections.unmodifiableMap(webClientMap);
    }
}
