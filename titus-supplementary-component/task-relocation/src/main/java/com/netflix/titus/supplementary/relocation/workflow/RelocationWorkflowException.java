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

package com.netflix.titus.supplementary.relocation.workflow;

public class RelocationWorkflowException extends RuntimeException {

    public enum ErrorCode {
        NotReady,
        StoreError,
    }

    private final ErrorCode errorCode;

    private RelocationWorkflowException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static RelocationWorkflowException notReady() {
        return new RelocationWorkflowException(ErrorCode.NotReady, "Relocation workflow not ready yet", null);
    }

    public static RelocationWorkflowException storeError(String message, Throwable cause) {
        return new RelocationWorkflowException(ErrorCode.StoreError, message, cause);
    }
}
