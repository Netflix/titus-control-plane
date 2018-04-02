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

package com.netflix.titus.ext.cassandra.executor;

/**
 */
public class Cell {
    private final String rowId;
    private final String column;
    private final Object value;

    public Cell(String rowId, String column, Object value) {
        this.rowId = rowId;
        this.column = column;
        this.value = value;
    }

    public String getRowId() {
        return rowId;
    }

    public String getColumn() {
        return column;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "rowId='" + rowId + '\'' +
                ", column='" + column + '\'' +
                ", value=" + value +
                '}';
    }
}
