package io.netflix.titus.ext.eureka.resolver;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.netflix.appinfo.AmazonInfo;
import com.netflix.appinfo.InstanceInfo;
import com.netflix.discovery.CacheRefreshedEvent;
import com.netflix.discovery.EurekaClient;
import com.netflix.discovery.EurekaEvent;
import com.netflix.discovery.EurekaEventListener;
import com.netflix.discovery.shared.Application;
import io.netflix.titus.common.util.NetworkExt;
import io.netflix.titus.runtime.endpoint.resolver.HostCallerIdResolver;

import static io.netflix.titus.common.util.StringExt.splitByComma;

/**
 */
@Singleton
public class EurekaHostCallerIdResolver implements HostCallerIdResolver, EurekaEventListener {

    public static final String OFFICE_IP_RANGES = "officeIpRanges";

    private static final String UNKNOWN_APP = "UNKNOWN";
    private static final String OFFICE = "OFFICE";

    private final EurekaClient eurekaClient;
    private volatile Map<String, String> addressToApplicationMap;
    private final Function<String, Boolean> officeNetworkPredicate;

    @Inject
    public EurekaHostCallerIdResolver(EurekaClient eurekaClient, @Named(OFFICE_IP_RANGES) String officeIpRanges) {
        this.eurekaClient = eurekaClient;
        this.officeNetworkPredicate = NetworkExt.buildNetworkMatchPredicate(splitByComma(officeIpRanges));
        refreshAddressCache();
        eurekaClient.registerEventListener(this);
    }

    @Override
    public Optional<String> resolve(String ipOrHostName) {
        String sourceApp = null;
        if (ipOrHostName != null) {
            sourceApp = addressToApplicationMap.get(ipOrHostName);
            if (sourceApp == null) {
                sourceApp = officeNetworkPredicate.apply(ipOrHostName) ? OFFICE : UNKNOWN_APP;
            }
        }
        return Optional.ofNullable(sourceApp);
    }

    @Override
    public void onEvent(EurekaEvent event) {
        if (event instanceof CacheRefreshedEvent) {
            refreshAddressCache();
        }
    }

    private void refreshAddressCache() {
        Map<String, String> newAddressMap = new HashMap<>();
        for (Application application : eurekaClient.getApplications().getRegisteredApplications()) {
            application.getInstances().forEach(ii -> appendApplicationAddresses(newAddressMap, ii));
        }
        this.addressToApplicationMap = newAddressMap;
    }

    private void appendApplicationAddresses(Map<String, String> newAddressMap, InstanceInfo ii) {
        String appName = ii.getAppName();
        if (appName == null) {
            return;
        }
        Consumer<String> addNonNull = address -> {
            if (address != null) {
                newAddressMap.put(address, appName);
            }
        };
        if (ii.getDataCenterInfo() instanceof AmazonInfo) {
            AmazonInfo amazonInfo = (AmazonInfo) ii.getDataCenterInfo();
            addNonNull.accept(amazonInfo.get(AmazonInfo.MetaDataKey.localHostname));
            addNonNull.accept(amazonInfo.get(AmazonInfo.MetaDataKey.localIpv4));
            addNonNull.accept(amazonInfo.get(AmazonInfo.MetaDataKey.publicHostname));
            addNonNull.accept(amazonInfo.get(AmazonInfo.MetaDataKey.publicIpv4));
        } else {
            addNonNull.accept(ii.getIPAddr());
            addNonNull.accept(ii.getHostName());
        }
    }
}
