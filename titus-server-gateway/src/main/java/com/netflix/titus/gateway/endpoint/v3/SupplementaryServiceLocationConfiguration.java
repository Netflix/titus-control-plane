package com.netflix.titus.gateway.endpoint.v3;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.archaius.api.Config;
import com.netflix.titus.common.util.PropertiesExt;

@Singleton
public class SupplementaryServiceLocationConfiguration {

    private static final String PREFIX = "titusGateway.supplementary.services";

    private final Map<String, ServiceAddress> serviceMap;

    @Inject
    public SupplementaryServiceLocationConfiguration(Config config) {
        Config serviceConfig = config.getPrefixedView(PREFIX);

        Map<String, String> all = new HashMap<>();
        serviceConfig.forEachProperty((k, v) -> all.put(k, v.toString()));

        Map<String, Map<String, String>> serviceProperties = PropertiesExt.groupByRootName(all, 1);

        Map<String, ServiceAddress> serviceMap = new HashMap<>();
        serviceProperties.forEach((k, v) -> {
            serviceMap.put(k, new ServiceAddress(v.get("host"), Integer.parseInt(v.get("grpcPort")), Integer.parseInt(v.get("httpPort"))));
        });

        this.serviceMap = Collections.unmodifiableMap(serviceMap);
    }

    public Map<String, ServiceAddress> getServices() {
        return serviceMap;
    }

    class ServiceAddress {
        private final String host;
        private final int grpcPort;
        private final int httpPort;

        public ServiceAddress(String host, int grpcPort, int httpPort) {
            this.host = host;
            this.grpcPort = grpcPort;
            this.httpPort = httpPort;
        }

        public String getHost() {
            return host;
        }

        public int getGrpcPort() {
            return grpcPort;
        }

        public int getHttpPort() {
            return httpPort;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServiceAddress that = (ServiceAddress) o;
            return grpcPort == that.grpcPort &&
                    httpPort == that.httpPort &&
                    Objects.equals(host, that.host);
        }

        @Override
        public int hashCode() {
            return Objects.hash(host, grpcPort, httpPort);
        }

        @Override
        public String toString() {
            return "ServiceAddress{" +
                    "host='" + host + '\'' +
                    ", grpcPort=" + grpcPort +
                    ", httpPort=" + httpPort +
                    '}';
        }
    }
}
