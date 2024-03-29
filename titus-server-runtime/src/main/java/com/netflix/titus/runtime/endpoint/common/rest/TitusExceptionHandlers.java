/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.runtime.endpoint.common.rest;

import java.util.Collection;
import javax.servlet.http.HttpServletResponse;

import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizerUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.netflix.titus.runtime.endpoint.rest.ErrorResponse;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
@Order(Ordered.HIGHEST_PRECEDENCE)
public class TitusExceptionHandlers {

    private static final int TOO_MANY_REQUESTS = 429;

    @ExceptionHandler(value = {RestException.class})
    public ResponseEntity<ErrorResponse> handleException(RestException e, WebRequest request) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(e.getStatusCode(), e.getMessage())
                .clientRequest(request)
                .serverContext()
                .exceptionContext(e);
        ErrorResponse errorResponse = errorBuilder.build();
        return ResponseEntity.status(e.getStatusCode()).body(errorResponse);
    }

    @ExceptionHandler(value = {TitusServiceException.class})
    public ResponseEntity<ErrorResponse> handleException(TitusServiceException e, WebRequest request) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(request)
                .serverContext()
                .exceptionContext(e);

        switch (e.getErrorCode()) {
            case CELL_NOT_FOUND:
            case JOB_NOT_FOUND:
            case TASK_NOT_FOUND:
                errorBuilder.status(HttpServletResponse.SC_NOT_FOUND);
                break;
            case INVALID_ARGUMENT:
            case INVALID_JOB:
            case INVALID_PAGE_OFFSET:
            case NO_CALLER_ID:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            case INTERNAL:
            case UNEXPECTED:
            default:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        if (!CollectionsExt.isNullOrEmpty(e.getValidationErrors())) {
            errorBuilder.withContext(
                    "constraintViolations",
                    EntitySanitizerUtil.toStringMap((Collection) e.getValidationErrors())
            );
        }

        ErrorResponse errorResponse = errorBuilder.build();
        return ResponseEntity.status(errorResponse.getStatusCode()).body(errorResponse);
    }

    @ExceptionHandler(value = {JobManagerException.class})
    public ResponseEntity<ErrorResponse> handleException(JobManagerException e, WebRequest request) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(request)
                .serverContext()
                .exceptionContext(e);

        switch (e.getErrorCode()) {
            case JobCreateLimited:
                errorBuilder.status(TOO_MANY_REQUESTS);
                break;
            case JobNotFound:
            case TaskNotFound:
                errorBuilder.status(HttpServletResponse.SC_NOT_FOUND);
                break;
            case JobTerminating:
            case TaskTerminating:
            case UnexpectedJobState:
            case UnexpectedTaskState:
                errorBuilder.status(HttpServletResponse.SC_PRECONDITION_FAILED);
                break;
            case NotEnabled:
                errorBuilder.status(HttpServletResponse.SC_METHOD_NOT_ALLOWED);
                break;
            case InvalidContainerResources:
            case InvalidDesiredCapacity:
            case InvalidMaxCapacity:
            case NotServiceJob:
            case NotServiceJobDescriptor:
            case NotBatchJob:
            case NotBatchJobDescriptor:
            case BelowMinCapacity:
            case AboveMaxCapacity:
            case TaskJobMismatch:
            case SameJobIds:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            default:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        ErrorResponse errorResponse = errorBuilder.build();
        return ResponseEntity.status(errorResponse.getStatusCode()).body(errorResponse);
    }

    @ExceptionHandler(value = {EvictionException.class})
    public ResponseEntity<ErrorResponse> handleException(EvictionException e, WebRequest request) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(request)
                .serverContext()
                .exceptionContext(e);

        switch (e.getErrorCode()) {
            case BadConfiguration:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            case CapacityGroupNotFound:
            case TaskNotFound:
                errorBuilder.status(HttpServletResponse.SC_NOT_FOUND);
                break;
            case TaskNotScheduledYet:
            case TaskAlreadyStopped:
            case NoQuota:
                errorBuilder.status(HttpServletResponse.SC_FORBIDDEN);
                break;
            default:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        ErrorResponse errorResponse = errorBuilder.build();
        return ResponseEntity.status(errorResponse.getStatusCode()).body(errorResponse);
    }
}
