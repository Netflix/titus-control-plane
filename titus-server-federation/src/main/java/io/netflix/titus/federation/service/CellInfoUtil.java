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
package io.netflix.titus.federation.service;


import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import io.netflix.titus.api.federation.model.Cell;

public class CellInfoUtil {

    /*
        Extracts cells from a cell specification that takes the following form
        cell1=hostName1:7001;cell2=hostName2:7002
     */
    public static List<Cell> extractCellsFromCellSpecification(String cellsSpecification) {
        return Arrays.stream(cellsSpecification.split(";"))
                .filter(cellSpec -> cellSpec.contains("="))
                .map(cellSpec -> {
                    String[] cellParts = cellSpec.split("=");
                    return new Cell(cellParts[0], cellParts[1]);
                })
                .collect(Collectors.toList());
    }

}
