package com.netflix.titus.common.network.reverseproxy.grpc;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ReverseProxyServerCallHandler implements ServerCallHandler<Object, Object> {

    private static final Logger logger = LoggerFactory.getLogger(ReverseProxyServerCallHandler.class);

    private final ManagedChannel channel;
    private final String methodName;

    ReverseProxyServerCallHandler(ManagedChannel channel, String methodName) {
        this.channel = channel;
        this.methodName = methodName;
    }

    @Override
    public ServerCall.Listener<Object> startCall(ServerCall<Object, Object> serverCall, Metadata headers) {
        ClientCall<Object, Object> forwardedCall = channel.newCall(
                newMethodDescriptorBuilder()
                        .setType(MethodDescriptor.MethodType.UNARY)
                        .setFullMethodName(methodName)
                        .build(),
                CallOptions.DEFAULT
        );

        ClientCall.Listener<Object> clientCallListener = new ClientCall.Listener<Object>() {
            @Override
            public void onHeaders(Metadata headers) {
                serverCall.sendHeaders(headers);
            }

            @Override
            public void onMessage(Object message) {
                serverCall.sendMessage(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
                serverCall.close(status, trailers);
            }

            @Override
            public void onReady() {
                serverCall.request(1);
            }
        };

        ServerCall.Listener<Object> serverCallListener = new ServerCall.Listener<Object>() {

            private volatile boolean halfClosed = false;

            @Override
            public void onMessage(Object message) {
                forwardedCall.request(2);

                try {
                    forwardedCall.sendMessage(message);
                } catch (RuntimeException | Error e) {
                    throw cancelThrow(forwardedCall, e);
                }
                onHalfClose();
            }

            @Override
            public void onHalfClose() {
                if (!halfClosed) {
                    halfClosed = true;
                    try {
                        forwardedCall.halfClose();
                    } catch (RuntimeException | Error e) {
                        throw cancelThrow(forwardedCall, e);
                    }
                }
            }

            @Override
            public void onCancel() {
                try {
                    forwardedCall.cancel("Client request cancelled", null);
                } catch (RuntimeException | Error e) {
                    throw cancelThrow(forwardedCall, e);
                }
            }

            @Override
            public void onComplete() {
                onHalfClose();
            }

            @Override
            public void onReady() {
                try {
                    forwardedCall.request(1);
                } catch (RuntimeException | Error e) {
                    throw cancelThrow(forwardedCall, e);
                }
            }
        };

        forwardedCall.start(clientCallListener, new Metadata());
        serverCall.request(2);

        return serverCallListener;
    }

    /**
     * Based on Java/GRPC implementation.
     */
    private static RuntimeException cancelThrow(ClientCall<?, ?> call, Throwable t) {
        try {
            call.cancel(null, t);
        } catch (Throwable e) {
            logger.error("Exception encountered while closing call", e);
        }
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        } else if (t instanceof Error) {
            throw (Error) t;
        }
        // should be impossible
        throw new AssertionError(t);
    }

    private static MethodDescriptor.Builder<Object, Object> newMethodDescriptorBuilder() {
        return MethodDescriptor.newBuilder()
                .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
                .setResponseMarshaller(ByteArrayMarshaller.INSTANCE);
    }
}
