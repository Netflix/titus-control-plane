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

import java.net.SocketException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.google.rpc.BadRequest;
import com.google.rpc.DebugInfo;
import com.netflix.titus.api.agent.service.AgentManagementException;
import com.netflix.titus.api.jobmanager.service.JobManagerException;
import com.netflix.titus.api.scheduler.service.SchedulerException;
import com.netflix.titus.api.service.TitusServiceException;
import com.netflix.titus.common.util.tuple.Pair;
import io.grpc.Metadata;
import io.grpc.Status;

import static java.util.Arrays.stream;

/**
 * GRPC layer errors.
 */
public final class ErrorResponses {

    public static final String X_TITUS_DEBUG = "X-Titus-Debug";
    public static final String X_TITUS_ERROR = "X-Titus-Error";
    public static final String X_TITUS_ERROR_BIN = "X-Titus-Error-bin";

    public static final Metadata.Key<String> KEY_TITUS_DEBUG = Metadata.Key.of(X_TITUS_DEBUG, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> KEY_TITUS_ERROR_REPORT = Metadata.Key.of(X_TITUS_ERROR, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> KEY_TITUS_ERROR_REPORT_BIN = Metadata.Key.of(X_TITUS_ERROR_BIN, Metadata.BINARY_BYTE_MARSHALLER);

    private ErrorResponses() {
    }

    public static Pair<Status, Metadata> of(Exception exception, boolean debug) {
        Status status = toGrpcStatus(exception)
                .withDescription(getNonNullMessage(exception))
                .withCause(exception);

        int errorCode = status.getCode().value();
        Metadata metadata = buildMetadata(exception, errorCode, debug);

        return Pair.of(status, metadata);
    }

    public static Pair<Status, Metadata> of(Status status, Metadata trailers, boolean debug) {
        Throwable cause = status.getCause();
        if (cause == null) {
            return Pair.of(status, trailers);
        }
        Status newStatus = toGrpcStatus(cause)
                .withDescription(getNonNullMessage(cause))
                .withCause(cause);

        Metadata metadata = buildMetadata(newStatus.getCause(), newStatus.getCode().value(), debug);
        metadata.merge(trailers);
        return Pair.of(newStatus, metadata);
    }

    private static Metadata buildMetadata(Throwable exception, int errorCode, boolean debug) {
        Metadata metadata = new Metadata();
        metadata.put(KEY_TITUS_ERROR_REPORT, getNonNullMessage(exception));
        if (debug) {
            metadata.put(KEY_TITUS_ERROR_REPORT_BIN, buildRpcStatus(exception, errorCode).toByteArray());
        }
        return metadata;
    }

    private static com.google.rpc.Status buildRpcStatus(Throwable exception, int errorCode) {
        com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
                .setCode(errorCode)
                .setMessage(getNonNullMessage(exception));

        DebugInfo debugInfo = DebugInfo.newBuilder()
                .addAllStackEntries(stream(exception.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()))
                .build();
        builder.addDetails(Any.pack(debugInfo));

        if (exception instanceof TitusServiceException) {
            TitusServiceException e = (TitusServiceException) exception;
            if (!e.getValidationErrors().isEmpty()) {
                BadRequest.Builder rbuilder = BadRequest.newBuilder();

                e.getValidationErrors().forEach(v -> {
                    BadRequest.FieldViolation.Builder fbuilder = BadRequest.FieldViolation.newBuilder();
                    fbuilder.setField(v.getField());
                    fbuilder.setDescription(v.getDescription());

                    rbuilder.addFieldViolations(fbuilder.build());
                });

                builder.addDetails(Any.pack(rbuilder.build()));
            }
        }

        return builder.build();
    }

    private static Status toGrpcStatus(Throwable cause) {
        if (cause instanceof SocketException) {
            return Status.UNAVAILABLE;
        } else if (cause instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED;
        } else if (cause instanceof TitusServiceException) {
            TitusServiceException e = (TitusServiceException) cause;
            switch (e.getErrorCode()) {
                case NOT_LEADER:
                    return Status.ABORTED;
                case NOT_READY:
                    return Status.UNAVAILABLE;
                case JOB_NOT_FOUND:
                case TASK_NOT_FOUND:
                    return Status.NOT_FOUND;
                case INVALID_ARGUMENT:
                    return Status.INVALID_ARGUMENT;
                case JOB_UPDATE_NOT_ALLOWED:
                    return Status.FAILED_PRECONDITION;
                case UNSUPPORTED_JOB_TYPE:
                    return Status.UNIMPLEMENTED;
                case INTERNAL:
                    return Status.INTERNAL;
                case NOT_SUPPORTED:
                case UNIMPLEMENTED:
                    return Status.UNIMPLEMENTED;
                case UNEXPECTED:
                    return Status.INTERNAL;
            }
        } else if (cause instanceof AgentManagementException) {
            AgentManagementException e = (AgentManagementException) cause;
            switch (e.getErrorCode()) {
                case InitializationError:
                    return Status.INTERNAL;
                case InvalidArgument:
                    return Status.INVALID_ARGUMENT;
                case InstanceGroupNotFound:
                case AgentNotFound:
                case InstanceTypeNotFound:
                    return Status.NOT_FOUND;
                default:
                    return Status.INTERNAL;
            }
        } else if (cause instanceof JobManagerException) {
            JobManagerException e = (JobManagerException) cause;
            switch (e.getErrorCode()) {
                case V2EngineTurnedOff:
                    return Status.PERMISSION_DENIED;
                case JobCreateLimited:
                    return Status.INVALID_ARGUMENT;
                case JobNotFound:
                case TaskNotFound:
                    return Status.NOT_FOUND;
                case JobTerminating:
                case TaskTerminating:
                case NotServiceJob:
                case UnexpectedJobState:
                case UnexpectedTaskState:
                    return Status.FAILED_PRECONDITION;
                case InvalidContainerResources:
                case InvalidDesiredCapacity:
                    return Status.INVALID_ARGUMENT;
            }
        } else if (cause instanceof SchedulerException) {
            SchedulerException e = (SchedulerException) cause;
            switch (e.getErrorCode()) {
                case InvalidArgument:
                case SystemSelectorAlreadyExists:
                case SystemSelectorEvaluationError:
                    return Status.INVALID_ARGUMENT;
                case SystemSelectorNotFound:
                    return Status.NOT_FOUND;
            }
        }
        return Status.INTERNAL;
    }

    private static String getNonNullMessage(Throwable e) {
        return e.getMessage() == null
                ? e.getClass().getSimpleName() + " (no message)"
                : e.getClass().getSimpleName() + ": " + e.getMessage();
    }
}
