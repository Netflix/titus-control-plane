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

package com.netflix.titus.runtime.connector.titusmaster;

import java.util.Optional;

import com.netflix.titus.common.util.rx.ReactorExt;
import reactor.core.publisher.Flux;
import rx.Observable;

public interface LeaderResolver {

    /**
     * @return {@link Address} of the leader
     */
    Optional<Address> resolve();

    /**
     * @return a persistent observable that emits the {@link Address} of the leader at some interval. The consumer should be able to handle
     * duplicates.
     * @deprecated Use {@link #observeLeaderFlux()} instead.
     */
    @Deprecated
    Observable<Optional<Address>> observeLeader();

    /**
     * Spring-Reactor variant of {@link #observeLeader()}.
     */
    default Flux<Optional<Address>> observeLeaderFlux() {
        return ReactorExt.toFlux(observeLeader());
    }
}
