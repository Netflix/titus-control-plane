package com.netflix.titus.common.network.reverseproxy.grpc;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.util.StringExt;
import io.grpc.HandlerRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;

@Singleton
public class RemoteHandlerRegistry extends HandlerRegistry {

    private static final String REVERSE_PROXY_SERVICE_NAME = "reverseProxy";
    private static final String REVERSE_PROXY_METHOD_NAME = REVERSE_PROXY_SERVICE_NAME + "/doForward";

    private static final MethodDescriptor<Object, Object> STREAMING_METHOD_DESCRIPTOR = MethodDescriptor.newBuilder()
            .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
            .setRequestMarshaller(ByteArrayMarshaller.INSTANCE)
            .setResponseMarshaller(ByteArrayMarshaller.INSTANCE)
            .setFullMethodName(REVERSE_PROXY_METHOD_NAME)
            .build();

    private final ManagedChannelFactory managedChannelFactory;

    @Inject
    public RemoteHandlerRegistry(ManagedChannelFactory managedChannelFactory) {
        this.managedChannelFactory = managedChannelFactory;
    }

    @Nullable
    @Override
    public ServerMethodDefinition<?, ?> lookupMethod(String methodName, @Nullable String authority) {
        return managedChannelFactory.newManagedChannel(StringExt.takeUntil(methodName, "/"))
                .map(c -> {
                    ServerMethodDefinition<Object, Object> methodDefinition = ServerMethodDefinition.create(
                            STREAMING_METHOD_DESCRIPTOR,
                            new ReverseProxyServerCallHandler(c, methodName)
                    );
                    ServerMethodDefinition serverMethodDefinition = ServerServiceDefinition.builder(REVERSE_PROXY_SERVICE_NAME)
                            .addMethod(methodDefinition)
                            .build()
                            .getMethod(REVERSE_PROXY_METHOD_NAME);

                    return serverMethodDefinition.withServerCallHandler(new ReverseProxyServerCallHandler(c, methodName));
                })
                .orElse(null);
    }
}
