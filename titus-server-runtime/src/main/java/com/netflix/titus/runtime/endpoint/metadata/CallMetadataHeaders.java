package com.netflix.titus.runtime.endpoint.metadata;

public final class CallMetadataHeaders {

    public final static String DEBUG_HEADER = "X-Titus-Debug";

    public final static String COMPRESSION_HEADER = "X-Titus-Compression";

    public final static String CALLER_ID_HEADER = "X-Titus-CallerId";

    public final static String DIRECT_CALLER_ID_HEADER = "X-Titus-DirectCallerId";

    public final static String CALL_REASON_HEADER = "X-Titus-CallReason";

    /**
     * For internal usage only (TitusFederation -> TitusGateway -> TitusMaster).
     */
    public static String CALL_METADATA_HEADER = "X-Titus-CallMetadata-bin";

    private CallMetadataHeaders() {
    }
}
