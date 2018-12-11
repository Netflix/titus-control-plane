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

package com.netflix.titus.runtime.endpoint.common.rest;

import java.net.SocketException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.netflix.titus.api.eviction.service.EvictionException;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.scheduler.service.SchedulerException;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.model.sanitizer.EntitySanitizerUtil;
import com.netflix.titus.common.util.CollectionsExt;
import com.sun.jersey.api.NotFoundException;
import com.sun.jersey.api.ParamException;

@Provider
public class TitusExceptionMapper implements ExceptionMapper<Throwable> {
    private static final int TOO_MANY_REQUESTS = 429;

    @Context
    private HttpServletRequest httpServletRequest;

    @Override
    public Response toResponse(Throwable exception) {
        if (exception instanceof SocketException) {
            return fromSocketException((SocketException) exception);
        }
        if (exception instanceof WebApplicationException) {
            return fromWebApplicationException((WebApplicationException) exception);
        }
        if (exception instanceof RestException) {
            return fromRestException((RestException) exception);
        }
        if (exception instanceof JsonProcessingException) {
            return fromJsonProcessingException((JsonProcessingException) exception);
        }
        if (exception instanceof TitusServiceException) {
            return fromTitusServiceException((TitusServiceException) exception);
        }
        if (exception instanceof JobManagerException) {
            return fromJobManagerException((JobManagerException) exception);
        }
        if (exception instanceof EvictionException) {
            return fromEvictionException((EvictionException) exception);
        }
        if (exception instanceof SchedulerException) {
            return fromSchedulerException((SchedulerException) exception);
        }
        if (exception instanceof TimeoutException) {
            return fromTimeoutException((TimeoutException) exception);
        }

        ErrorResponse errorResponse = ErrorResponse.newError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Unexpected error: " + exception.getMessage())
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(exception)
                .build();
        return Response.status(Status.INTERNAL_SERVER_ERROR).entity(errorResponse).build();
    }

    private Response fromSocketException(SocketException e) {
        int status = HttpServletResponse.SC_SERVICE_UNAVAILABLE;

        Throwable cause = e.getCause() == null ? e : e.getCause();
        String errorMessage = toStandardHttpErrorMessage(status, cause);

        ErrorResponse errorResponse = ErrorResponse.newError(status, errorMessage)
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(cause)
                .build();
        return Response.status(status).entity(errorResponse).build();
    }

    private Response fromRestException(RestException e) {
        ErrorResponse errorResponse = ErrorResponse.newError(e.getStatusCode(), e.getMessage())
                .errorDetails(e.getDetails())
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(e)
                .build();
        return Response.status(e.getStatusCode()).entity(errorResponse).build();
    }

    private Response fromWebApplicationException(WebApplicationException e) {
        int status = e.getResponse().getStatus();

        Throwable cause = e.getCause() == null ? e : e.getCause();
        String errorMessage = toStandardHttpErrorMessage(status, cause);

        ErrorResponse errorResponse = ErrorResponse.newError(status, errorMessage)
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(cause)
                .build();
        return Response.status(status).entity(errorResponse).build();
    }

    private String toStandardHttpErrorMessage(int status, Throwable cause) {
        // Do not use message from Jersey exceptions, as we can do better
        if (cause instanceof ParamException) {
            ParamException pe = (ParamException) cause;
            return "invalid parameter " + pe.getParameterName() + "=" + pe.getDefaultStringValue() + " of type " + pe.getParameterType();
        }
        if (cause instanceof NotFoundException) {
            NotFoundException nfe = (NotFoundException) cause;
            return "resource not found: " + nfe.getNotFoundUri();
        }
        if (cause.getMessage() != null) {
            return cause.getMessage();
        }
        try {
            return Status.fromStatusCode(status).getReasonPhrase();
        } catch (Exception e) {
            return "HTTP error " + status;
        }
    }

    private Response fromJsonProcessingException(JsonProcessingException e) {
        StringBuilder msgBuilder = new StringBuilder();
        if (e.getOriginalMessage() != null) {
            msgBuilder.append(e.getOriginalMessage());
        } else {
            msgBuilder.append("JSON processing error");
        }
        JsonLocation location = e.getLocation();
        if (location != null) {
            msgBuilder.append(" location: [line: ").append(location.getLineNr())
                    .append(", column: ").append(location.getColumnNr()).append(']');
        }

        ErrorResponse errorResponse = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, msgBuilder.toString())
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(e)
                .build();
        return Response.status(Status.BAD_REQUEST).entity(errorResponse).build();
    }

    private Response fromTitusServiceException(TitusServiceException e) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(e);

        switch (e.getErrorCode()) {
            case JOB_NOT_FOUND:
            case TASK_NOT_FOUND:
                errorBuilder.status(HttpServletResponse.SC_NOT_FOUND);
                break;
            case JOB_UPDATE_NOT_ALLOWED:
                Throwable cause = e.getCause();
                if (cause != null) {
                    errorBuilder.message(cause.getMessage()).exceptionContext(cause);
                }
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            case UNSUPPORTED_JOB_TYPE:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            case NOT_READY:
                errorBuilder.status(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
                break;
            case UNIMPLEMENTED:
            case INTERNAL:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            case UNEXPECTED:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            case INVALID_ARGUMENT:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
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
        return Response.status(errorResponse.getStatusCode()).entity(errorResponse).build();
    }

    private Response fromJobManagerException(JobManagerException e) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(httpServletRequest)
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
            // these below are mapped to Status.FAILED_PRECONDITION for gRPC calls
            case JobTerminating:
            case TaskTerminating:
            case UnexpectedJobState:
            case UnexpectedTaskState:
            case NotEnabled:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                break;
            case InvalidContainerResources:
            case InvalidDesiredCapacity:
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
        return Response.status(errorResponse.getStatusCode()).entity(errorResponse).build();
    }

    private Response fromEvictionException(EvictionException e) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(httpServletRequest)
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
        return Response.status(errorResponse.getStatusCode()).entity(errorResponse).build();
    }

    private Response fromSchedulerException(SchedulerException e) {
        ErrorResponse.ErrorResponseBuilder errorBuilder = ErrorResponse.newError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage())
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(e);

        switch (e.getErrorCode()) {
            case InvalidArgument:
            case SystemSelectorAlreadyExists:
            case SystemSelectorEvaluationError:
                errorBuilder.status(HttpServletResponse.SC_BAD_REQUEST);
                break;
            case SystemSelectorNotFound:
                errorBuilder.status(HttpServletResponse.SC_NOT_FOUND);
                break;
            default:
                errorBuilder.status(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }
        ErrorResponse errorResponse = errorBuilder.build();
        return Response.status(errorResponse.getStatusCode()).entity(errorResponse).build();
    }

    private Response fromTimeoutException(TimeoutException e) {
        int status = HttpServletResponse.SC_GATEWAY_TIMEOUT;

        Throwable cause = e.getCause() == null ? e : e.getCause();
        String errorMessage = toStandardHttpErrorMessage(status, cause);

        ErrorResponse errorResponse = ErrorResponse.newError(status, errorMessage)
                .clientRequest(httpServletRequest)
                .serverContext()
                .exceptionContext(cause)
                .build();
        return Response.status(status).entity(errorResponse).build();
    }
}
