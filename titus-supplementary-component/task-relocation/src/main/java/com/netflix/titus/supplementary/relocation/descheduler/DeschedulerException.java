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

package com.netflix.titus.supplementary.relocation.descheduler;

public class DeschedulerException extends RuntimeException {

    public enum ErrorCode {
        NoQuotaLeft
    }

    private final ErrorCode errorCode;

    private DeschedulerException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public static DeschedulerException noQuotaLeft(String message, Object... args) {
        String formatted = args.length > 0
                ? String.format(message, args)
                : message;
        return new DeschedulerException(ErrorCode.NoQuotaLeft, formatted);
    }
}
