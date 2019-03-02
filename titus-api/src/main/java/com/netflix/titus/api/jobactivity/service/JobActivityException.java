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

package com.netflix.titus.api.jobactivity.service;

import static java.lang.String.format;

public class JobActivityException extends RuntimeException {

    public enum ErrorCode {
        CREATE_ERROR,
        STORE_ERROR,
    }

    private final ErrorCode errorCode;

    private JobActivityException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static JobActivityException jobActivityCreateTableException(String tableName, Throwable cause) {
        return new JobActivityException(ErrorCode.CREATE_ERROR,
                format("Unable to create table %s: %s", tableName, cause.getMessage()),
                cause);
    }

    public static JobActivityException jobActivityUpdateRecordException(String jobId, Throwable cause) {
        return new JobActivityException(ErrorCode.STORE_ERROR,
                format("Unable to store job id %s to publisher: %s", jobId, cause.getMessage()),
                cause);
    }
}
