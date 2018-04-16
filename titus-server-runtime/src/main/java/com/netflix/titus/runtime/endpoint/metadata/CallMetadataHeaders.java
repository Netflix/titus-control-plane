package com.netflix.titus.runtime.endpoint.metadata;

public final class CallMetadataHeaders {

    public static String DEBUG_HEADER = "X-Titus-Debug";

    public static String CALLER_ID_HEADER = "X-Titus-CallerId";

    public static String DIRECT_CALLER_ID_HEADER = "X-Titus-DirectCallerId";

    public static String CALL_REASON_HEADER = "X-Titus-CallReason";

    /**
     * For internal usage only (TitusFederation -> TitusGateway -> TitusMaster).
     */
    public static String CALL_METADATA_HEADER = "X-Titus-CallMetadata-bin";

    private CallMetadataHeaders() {
    }
}
