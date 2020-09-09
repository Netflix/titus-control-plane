/*
 * Copyright 2020 Netflix, Inc.
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

package com.netflix.titus.client.clustermembership.resolver;

import com.netflix.titus.api.clustermembership.model.ClusterMember;

/**
 * {@link ClusterMemberVerifier} verifies that a discovered cluster member belongs to the desired membership group.
 * This unlikely event may happen, if an IP address is reused by another stack of the same application before the
 * the state is cleaned up by the client. The client when reconnecting would connect to a newly created instance of
 * the same application but in a different stack.
 */
public interface ClusterMemberVerifier {

    ClusterMemberVerifierResult verify(ClusterMember clusterMember);
}
