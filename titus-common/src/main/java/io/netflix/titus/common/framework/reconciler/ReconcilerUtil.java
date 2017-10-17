/*
 * Copyright 2017 Netflix, Inc.
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

package io.netflix.titus.common.framework.reconciler;

import org.slf4j.Logger;
import rx.Observable;

/**
 */
public final class ReconcilerUtil {

    private ReconcilerUtil() {
    }

    public static void logEvents(Observable<ReconcilerEvent> events, Logger logger) {
        events.subscribe(
                next -> logger.info(next.toString()),
                e -> logger.warn("Reconciler event stream terminated with an error", e),
                () -> logger.info("Reconciler event stream completed")
        );
    }
}
