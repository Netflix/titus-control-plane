/*
 * Copyright 2019 Netflix, Inc.
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

package com.netflix.titus.api.model.callmetadata;

public class CallMetadataConstants {

    /**
     * Call metadata for an undefined caller.
     */
    public static final CallMetadata UNDEFINED_CALL_METADATA = CallMetadata.newBuilder().withCallerId("Unknown").withCallReason("Unknown").build();

    /**
     * Call metadata for replicator event stream
     */
    public static final CallMetadata GRPC_REPLICATOR_CALL_METADATA = CallMetadata.newBuilder().withCallerId("ReplicatorEvent").withCallReason("Replication").build();
}
