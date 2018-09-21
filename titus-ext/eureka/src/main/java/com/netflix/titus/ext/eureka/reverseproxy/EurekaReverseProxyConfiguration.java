package com.netflix.titus.ext.eureka.reverseproxy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.archaius.api.Config;
import com.netflix.titus.common.util.PropertiesExt;

@Singleton
public class EurekaReverseProxyConfiguration {

    private static final String PREFIX = "titusGateway.supplementary.services";

    private final Map<String, EurekaServiceAddress> serviceMap;

    @Inject
    public EurekaReverseProxyConfiguration(Config config) {
        Config serviceConfig = config.getPrefixedView(PREFIX);

        Map<String, String> all = new HashMap<>();
        serviceConfig.forEachProperty((k, v) -> all.put(k, v.toString()));

        Map<String, Map<String, String>> serviceProperties = PropertiesExt.groupByRootName(all, 1);

        Map<String, EurekaServiceAddress> serviceMap = new HashMap<>();
        serviceProperties.forEach((k, v) -> {
            serviceMap.put(k, new EurekaServiceAddress(v.get("vipAddress"), Boolean.TRUE.equals(v.getOrDefault("secure", "false"))));
        });

        this.serviceMap = Collections.unmodifiableMap(serviceMap);
    }

    public Map<String, EurekaServiceAddress> getServiceWithVipAddresses() {
        return serviceMap;
    }

    public static class EurekaServiceAddress {

        private final String vipAddress;
        private final boolean isSecure;

        public EurekaServiceAddress(String vipAddress, boolean isSecure) {
            this.vipAddress = vipAddress;
            this.isSecure = isSecure;
        }

        public String getVipAddress() {
            return vipAddress;
        }

        public boolean isSecure() {
            return isSecure;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            EurekaServiceAddress that = (EurekaServiceAddress) o;
            return isSecure == that.isSecure &&
                    Objects.equals(vipAddress, that.vipAddress);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vipAddress, isSecure);
        }

        @Override
        public String toString() {
            return "EurekaServiceAddress{" +
                    "vipAddress='" + vipAddress + '\'' +
                    ", isSecure=" + isSecure +
                    '}';
        }
    }
}
