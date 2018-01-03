package io.netflix.titus.testkit.embedded.cloud.connector.remote;

import io.netflix.titus.common.util.tuple.Pair;

/**
 * Resolves host/port number of cloud simulator.
 */
public interface CloudSimulatorResolver {

    /**
     * Returns host/port number pair.
     */
    Pair<String, Integer> resolveGrpcEndpoint();
}
