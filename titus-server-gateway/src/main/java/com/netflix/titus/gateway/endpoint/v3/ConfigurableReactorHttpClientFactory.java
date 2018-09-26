package com.netflix.titus.gateway.endpoint.v3;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.titus.common.network.reverseproxy.http.ReactorHttpClientFactory;
import reactor.ipc.netty.http.client.HttpClient;

@Singleton
public class ConfigurableReactorHttpClientFactory implements ReactorHttpClientFactory {

    private final Map<String, HttpClient> clients;

    @Inject
    public ConfigurableReactorHttpClientFactory(SupplementaryServiceLocationConfiguration configuration) {
        this.clients = buildClients(configuration);
    }

    @Override
    public Optional<HttpClient> newHttpClient(String path) {
        for (Map.Entry<String, HttpClient> entry : clients.entrySet()) {
            if (path.contains(entry.getKey())) {
                return Optional.of(entry.getValue());
            }
        }
        return Optional.empty();
    }

    private Map<String, HttpClient> buildClients(SupplementaryServiceLocationConfiguration configuration) {
        Map<String, HttpClient> result = new HashMap<>();
        configuration.getServices().forEach((k, v) -> result.put(k, HttpClient.create(v.getHost(), v.getHttpPort())));
        return result;
    }
}
