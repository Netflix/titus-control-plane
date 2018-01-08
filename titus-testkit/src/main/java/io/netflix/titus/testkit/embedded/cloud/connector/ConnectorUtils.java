package io.netflix.titus.testkit.embedded.cloud.connector;

import java.util.Collection;

import io.netflix.titus.common.util.CollectionsExt;
import org.apache.mesos.Protos;

public final class ConnectorUtils {

    /**
     * Because of race condition in Fenzo, it may happen that an expired lease is still hold by it. In this case we take
     * the last one, and depend on teh fact that all leases provided here will be auto-expired by Fenzo.
     * <h1>Fenzo race condition</h1>
     * Fenzo processes new offers/leases and lease decline on internal event loop, just before doing task placement.
     * First declined requests are processed, and next new offers are added. This is a problem when a lease is quickly
     * offered and declined, and both requests are handled in the same cycle:
     * <ul>
     * <li>a decline request is processed first, but there is no such lease in Fenzo, so the request is void</li>
     * <li>an add request is processed next, and the lease that was just unsuccessfully declined is added to Fenzo</li>
     * </ul>
     */
    public static String findLatestLease(Collection<Protos.OfferID> offerIds) {
        return CollectionsExt.last(offerIds.stream().map(Protos.OfferID::getValue).sorted(String::compareTo));
    }
}
