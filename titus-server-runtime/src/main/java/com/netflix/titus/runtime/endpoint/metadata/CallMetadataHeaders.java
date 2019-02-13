/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.runtime.endpoint.metadata;

import io.grpc.CompressorRegistry;

public final class CallMetadataHeaders {

    /**
     * If set to true, additional debug information is returned for failing requests.
     */
    public final static String DEBUG_HEADER = "X-Titus-Debug";

    /**
     * Set compression type. The default GRPC compressor is 'gzip', so it is safe to assume it is always supported.
     * Other compressor types may require additional dependencies/configuration with the compressor registry
     * {@link CompressorRegistry}.
     */
    public final static String COMPRESSION_HEADER = "X-Titus-Compression";

    /*
     * Caller authentication/authorization headers.
     */

    /**
     * The original caller id. When a request is forwarded through multiple intermediaries, this header
     * should include the identity of a user/application that originates the request.
     */
    public final static String CALLER_ID_HEADER = "X-Titus-CallerId";

    /**
     * Type of a caller id set in {@link #CALLER_ID_HEADER}.
     */
    public final static String CALLER_TYPE_HEADER = "X-Titus-CallerType";

    /**
     * The identity of an entity making direct call to Titus without any intermediaries in between.
     * <h1>Limitations</h1>
     * The current model allows to capture a single intermediary only.
     */
    public final static String DIRECT_CALLER_ID_HEADER = "X-Titus-DirectCallerId";

    /**
     * Type of a caller id set in {@link #DIRECT_CALLER_ID_HEADER}.
     */
    public final static String DIRECT_CALLER_TYPE_HEADER = "X-Titus-DirectCallerType";

    /**
     * A reason for making the call. The value if set is part of the transaction log/activity history.
     */
    public final static String CALL_REASON_HEADER = "X-Titus-CallReason";

    /**
     * For internal usage only (TitusFederation -> TitusGateway -> TitusMaster).
     */
    public static String CALL_METADATA_HEADER = "X-Titus-CallMetadata-bin";

    /*
     * Direct caller context data.
     */

    /**
     * Direct caller server group stack name.
     */
    public static final String DIRECT_CALLER_CONTEXT_STACK = "titus.serverGroup.stack";

    /**
     * Direct caller server group detail.
     */
    public static final String DIRECT_CALLER_CONTEXT_DETAIL = "titus.serverGroup.detail";

    /**
     * Direct caller instance id.
     */
    public static final String DIRECT_CALLER_CONTEXT_INSTANCE_ID = "titus.caller.instanceId";

    /**
     * Name of the service that is invoked.
     */
    public static final String DIRECT_CALLER_CONTEXT_SERVICE_NAME = "titus.service.name";

    /**
     * Name of the method that is invoked.
     */
    public static final String DIRECT_CALLER_CONTEXT_SERVICE_METHOD = "titus.service.method";

    /**
     * Transport type (HTTP/GRPC).
     */
    public static final String DIRECT_CALLER_CONTEXT_TRANSPORT_TYPE = "titus.transport.type";

    /**
     * True for TLS, false for plain text.
     */
    public static final String DIRECT_CALLER_CONTEXT_TRANSPORT_SECURE = "titus.transport.secure";

    private CallMetadataHeaders() {
    }
}
