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

public class CellFederationException extends RuntimeException {

    public enum ErrorCode {
        CellNotFound,
        InvalidCellConfig,
    }

    private final ErrorCode errorCode;

    public CellFederationException(ErrorCode errorCode, String message, Throwable cause, Object... args) {
        super(String.format(message, args), cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static CellFederationException cellNotFound(String cellName) {
        return new CellFederationException(ErrorCode.CellNotFound, "Cell %s not found", null, cellName);
    }

    public static CellFederationException invalidCellConfig(String message, Object... args) {
        throw new CellFederationException(ErrorCode.InvalidCellConfig, message, null, args);
    }
}
