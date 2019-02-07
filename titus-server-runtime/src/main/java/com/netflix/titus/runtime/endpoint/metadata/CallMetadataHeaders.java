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

public final class CallMetadataHeaders {

    public final static String DEBUG_HEADER = "X-Titus-Debug";

    public final static String COMPRESSION_HEADER = "X-Titus-Compression";

    public final static String CALLER_ID_HEADER = "X-Titus-CallerId";

    public final static String CALLER_TYPE_HEADER = "X-Titus-CallerType";

    public final static String DIRECT_CALLER_ID_HEADER = "X-Titus-DirectCallerId";

    public final static String CALL_REASON_HEADER = "X-Titus-CallReason";

    /**
     * For internal usage only (TitusFederation -> TitusGateway -> TitusMaster).
     */
    public static String CALL_METADATA_HEADER = "X-Titus-CallMetadata-bin";

    private CallMetadataHeaders() {
    }
}
