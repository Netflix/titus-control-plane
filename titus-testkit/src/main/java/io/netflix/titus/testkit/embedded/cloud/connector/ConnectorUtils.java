package io.netflix.titus.testkit.embedded.cloud.connector;

import java.util.Collection;

import com.google.common.base.Preconditions;
import org.apache.mesos.Protos;

public final class ConnectorUtils {

    /**
     * Because of race condition in Fenzo, it may happen that an expired lease is still hold by it. In this case we take
     * the first one (oldest), which should be invalid on an agent, so invalid lease error is sent back to the client.
     * <p>
     * <h1>Fenzo race condition</h1>
     * Fenzo processes new offers/leases and lease decline on internal event loop, just before doing task placement.
     * First declined requests are processed, and next new offers are added. This is a problem when a lease is quickly
     * offered and declined, and both requests are handled in the same cycle:
     * <ul>
     * <li>a decline request is processed first, but there is no such lease in Fenzo, so the request is void</li>
     * <li>an add request is processed next, and the lease that was just unsuccessfully declined is added to Fenzo</li>
     * </ul>
     */
    public static String findEarliestLease(Collection<Protos.OfferID> offerIds) {
        Preconditions.checkArgument(!offerIds.isEmpty(), "Expected at least one offer");
        return offerIds.stream().map(Protos.OfferID::getValue).sorted(String::compareTo).findFirst().get();
    }
}
