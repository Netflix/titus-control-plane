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

package com.netflix.titus.runtime.endpoint.v3.grpc;

import java.util.Collections;

import com.google.common.collect.ImmutableMap;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.runtime.SystemLogEvent;
import com.netflix.titus.common.runtime.SystemLogService;
import com.netflix.titus.grpc.protogen.Page;
import com.netflix.titus.runtime.endpoint.metadata.CallMetadataResolver;
import io.grpc.stub.StreamObserver;

/**
 * Helper method for APIs using pagination.
 */
public class TitusPaginationUtils {
    public static boolean checkPageIsValid(Page page, StreamObserver<?> responseObserver) {
        if (page == null) {
            responseObserver.onError(TitusServiceException.invalidArgument("Page not defined for the query"));
            return false;
        }
        if (page.getPageSize() <= 0) {
            responseObserver.onError(TitusServiceException.invalidArgument("Page size must be > 0 (is " + page.getPageSize() + ')'));
            return false;
        }
        if (page.getPageNumber() < 0) {
            responseObserver.onError(TitusServiceException.invalidArgument("Page number must be >= 0 (is " + page.getPageNumber() + ')'));
            return false;
        }
        return true;
    }

    public static void logPageNumberUsage(SystemLogService systemLog,
                                          CallMetadataResolver metadataResolver,
                                          String component,
                                          String apiName,
                                          Page page) {
        if (page.getPageNumber() == 0) {
            return;
        }

        ImmutableMap.Builder<String, String> contextBuilder = new ImmutableMap.Builder<>();
        contextBuilder.put("apiName", apiName);
        metadataResolver.resolve().ifPresent(callMetadata -> contextBuilder
                .put("callPath", String.join(",", callMetadata.getCallPath()))
                .put("callerId", callMetadata.getCallerId())
                .put("reason", callMetadata.getCallReason())
        );

        systemLog.submit(SystemLogEvent.newBuilder()
                .withCategory(SystemLogEvent.Category.Other)
                .withComponent(component)
                .withPriority(SystemLogEvent.Priority.Info)
                .withTags(Collections.singleton("pageNumberUsage"))
                .withContext(contextBuilder.build())
                .withMessage("API called with a pageNumber")
                .build()
        );
    }
}
