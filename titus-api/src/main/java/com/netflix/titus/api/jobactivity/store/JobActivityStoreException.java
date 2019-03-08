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

package com.netflix.titus.api.jobactivity.store;

import static java.lang.String.format;

public class JobActivityStoreException extends RuntimeException {

    public enum ErrorCode {
        CREATE_ERROR,
        STORE_ERROR,
    }

    private final ErrorCode errorCode;

    private JobActivityStoreException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static JobActivityStoreException jobActivityCreateTableException(String tableName, Throwable cause) {
        return new JobActivityStoreException(ErrorCode.CREATE_ERROR,
                format("Unable to create table %s: %s", tableName, cause.getMessage()),
                cause);
    }

    public static JobActivityStoreException jobActivityUpdateRecordException(String jobId, Throwable cause) {
        return new JobActivityStoreException(ErrorCode.STORE_ERROR,
                format("Unable to store job/task id %s to publisher: %s", jobId, cause.getMessage()),
                cause);
    }
}
