package io.netflix.titus.gateway.service.v3.internal;

final class GrpcServiceUtil {

    /**
     * For request/response GRPC calls, we set execution deadline at both RxJava and GRPC level. As we prefer the timeout
     * be triggered by GRPC, which may give us potentially more insight, we adjust RxJava timeout value by this factor.
     */
    static double RX_CLIENT_TIMEOUT_FACTOR = 1.2;

    static long getRxJavaAdjustedTimeout(long initialTimeoutMs) {
        return (long) (initialTimeoutMs * RX_CLIENT_TIMEOUT_FACTOR);
    }
}
