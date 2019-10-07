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

package com.netflix.titus.runtime.endpoint.common.grpc.interceptor;

import com.netflix.titus.common.util.Evaluators;
import com.netflix.titus.common.util.tuple.Pair;
import com.netflix.titus.runtime.endpoint.v3.grpc.ErrorResponses;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.titus.common.util.Evaluators.getOrDefault;
import static com.netflix.titus.runtime.endpoint.v3.grpc.ErrorResponses.KEY_TITUS_DEBUG;

/**
 * (adapted from netflix-grpc-extensions)
 * <p>
 * Interceptor that ensures any exception thrown by a method handler is propagated
 * as a close() to all upstream {@link ServerInterceptor}s.
 * Custom exceptions mapping can be provided through customMappingFunction.
 *
 * @deprecated Use {@link CommonErrorCatchingServerInterceptor}
 */
public final class ErrorCatchingServerInterceptor implements ServerInterceptor {

    private static final Logger logger = LoggerFactory.getLogger(ErrorCatchingServerInterceptor.class);

    private <ReqT, RespT> void handlingException(ServerCall<ReqT, RespT> call, Exception e, boolean debug) {
        logger.info("Returning exception to the client: {}", e.getMessage(), e);
        Pair<Status, Metadata> statusAndMeta = ErrorResponses.of(e, debug);
        Status status = statusAndMeta.getLeft();
        safeClose(() -> call.close(status, statusAndMeta.getRight()));
        throw status.asRuntimeException();
    }

    @Override
    public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                      ServerCallHandler<ReqT, RespT> next) {
        boolean debug = "true".equalsIgnoreCase(getOrDefault(headers.get(KEY_TITUS_DEBUG), "false"));
        Listener<ReqT> listener = null;
        try {
            listener = next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                @Override
                public void close(Status status, Metadata trailers) {
                    if (status.getCode() != Status.Code.OK) {
                        Pair<Status, Metadata> pair = ErrorResponses.of(status, trailers, debug);
                        Status newStatus = pair.getLeft();
                        if (isCriticalError(newStatus)) {
                            logger.warn("Returning exception to the client: {}", formatStatus(newStatus));
                        } else {
                            logger.debug("Returning exception to the client: {}", formatStatus(newStatus));
                        }
                        Evaluators.acceptNotNull(newStatus.getCause(), error -> logger.debug("Stack trace", error));

                        safeClose(() -> super.close(newStatus, pair.getRight()));
                    }
                    safeClose(() -> super.close(status, trailers));
                }
            }, headers);
        } catch (Exception e) {
            handlingException(call, e, debug);
        }
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(listener) {
            // Clients sends one requests are handled through onHalfClose() (not onMessage)
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (Exception e) {
                    handlingException(call, e, debug);
                }
            }

            // Streaming client requests are handled in onMessage()
            @Override
            public void onMessage(ReqT message) {
                try {
                    super.onMessage(message);
                } catch (Exception e) {
                    handlingException(call, e, debug);
                }
            }
        };
    }

    private boolean isCriticalError(Status status) {
        switch (status.getCode()) {
            case OK:
            case CANCELLED:
            case INVALID_ARGUMENT:
            case NOT_FOUND:
            case ALREADY_EXISTS:
            case PERMISSION_DENIED:
            case FAILED_PRECONDITION:
            case OUT_OF_RANGE:
            case UNAUTHENTICATED:
                return false;
            case UNKNOWN:
            case DEADLINE_EXCEEDED:
            case RESOURCE_EXHAUSTED:
            case ABORTED:
            case UNIMPLEMENTED:
            case INTERNAL:
            case UNAVAILABLE:
            case DATA_LOSS:
                return true;
        }
        // In case we missed something
        return true;
    }

    private String formatStatus(Status status) {
        return "{code=" + status.getCode()
                + ", description=" + status.getDescription()
                + ", error=" + (status.getCause() == null ? "N/A" : status.getCause().getMessage())
                + '}';
    }

    private void safeClose(Runnable action) {
        try {
            action.run();
        } catch (IllegalStateException ignore) {
            // Ignore, as most likely connection is already closed
        }
    }
}
