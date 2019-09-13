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

package com.netflix.titus.runtime.endpoint.common.grpc;

import java.net.SocketException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.Any;
import com.google.rpc.DebugInfo;
import com.netflix.titus.common.util.tuple.Pair;
import io.grpc.Metadata;
import io.grpc.Status;
import rx.exceptions.CompositeException;

import static java.util.Arrays.stream;

/**
 * TODO Map data validation errors to FieldViolation GRPC message, after the validation framework is moved to titus-common.
 */
public class GrpcExceptionMapper {

    public static final String X_TITUS_DEBUG = "X-Titus-Debug";
    public static final String X_TITUS_ERROR = "X-Titus-Error";
    public static final String X_TITUS_ERROR_BIN = "X-Titus-Error-bin";

    public static final Metadata.Key<String> KEY_TITUS_DEBUG = Metadata.Key.of(X_TITUS_DEBUG, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<String> KEY_TITUS_ERROR_REPORT = Metadata.Key.of(X_TITUS_ERROR, Metadata.ASCII_STRING_MARSHALLER);
    public static final Metadata.Key<byte[]> KEY_TITUS_ERROR_REPORT_BIN = Metadata.Key.of(X_TITUS_ERROR_BIN, Metadata.BINARY_BYTE_MARSHALLER);

    private final List<Function<Throwable, Optional<Status>>> serviceExceptionMappers;

    public GrpcExceptionMapper(List<Function<Throwable, Optional<Status>>> serviceExceptionMappers) {
        this.serviceExceptionMappers = serviceExceptionMappers;
    }

    public Pair<Status, Metadata> of(Throwable t, boolean debug) {
        Throwable exception = unwrap(t);
        Status status = toGrpcStatus(exception)
                .withDescription(getNonNullMessage(exception))
                .withCause(exception);

        int errorCode = status.getCode().value();
        Metadata metadata = buildMetadata(exception, errorCode, debug);

        return Pair.of(status, metadata);
    }

    public Pair<Status, Metadata> of(Status status, Metadata trailers, boolean debug) {
        Throwable cause = unwrap(status.getCause());
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

    private Metadata buildMetadata(Throwable exception, int errorCode, boolean debug) {
        Metadata metadata = new Metadata();
        metadata.put(KEY_TITUS_ERROR_REPORT, getNonNullMessage(exception));
        if (debug) {
            metadata.put(KEY_TITUS_ERROR_REPORT_BIN, buildRpcStatus(exception, errorCode).toByteArray());
        }
        return metadata;
    }

    private com.google.rpc.Status buildRpcStatus(Throwable exception, int errorCode) {
        com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
                .setCode(errorCode)
                .setMessage(getNonNullMessage(exception));

        DebugInfo debugInfo = DebugInfo.newBuilder()
                .addAllStackEntries(stream(exception.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList()))
                .build();
        builder.addDetails(Any.pack(debugInfo));
        return builder.build();
    }

    private Status toGrpcStatus(Throwable original) {
        Throwable cause = unwrap(original);
        if (cause instanceof SocketException) {
            return Status.UNAVAILABLE;
        } else if (cause instanceof TimeoutException) {
            return Status.DEADLINE_EXCEEDED;
        }
        for (Function<Throwable, Optional<Status>> mapper : serviceExceptionMappers) {
            Status status = mapper.apply(cause).orElse(null);
            if (status != null) {
                return status;
            }
        }
        return Status.INTERNAL;
    }

    private static String getNonNullMessage(Throwable t) {
        Throwable e = unwrap(t);
        return e.getMessage() == null
                ? e.getClass().getSimpleName() + " (no message)"
                : e.getClass().getSimpleName() + ": " + e.getMessage();
    }

    private static Throwable unwrap(Throwable throwable) {
        if (throwable instanceof CompositeException) {
            CompositeException composite = (CompositeException) throwable;
            if (composite.getExceptions().size() == 1) {
                return composite.getExceptions().get(0);
            }
        }
        return throwable;
    }
}
