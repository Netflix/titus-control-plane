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

package com.netflix.titus.testkit.cli.command;

import com.netflix.titus.testkit.grpc.GrpcClientErrorUtils;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ErrorReports {

    private static final Logger logger = LoggerFactory.getLogger(ErrorReports.class);

    public static void handleReplyError(String message, Throwable e) {
        logger.error(message + ": " + e.getMessage());
        if (e instanceof StatusRuntimeException) {
            GrpcClientErrorUtils.printDetails((StatusRuntimeException) e);
        }
        logger.info("stack trace", e);
    }
}
