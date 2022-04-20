/*
 * Copyright 2022 Netflix, Inc.
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

package com.netflix.titus.common.util.loadshedding.backoff;

import java.util.concurrent.TimeUnit;

import com.netflix.titus.common.runtime.TitusRuntime;
import com.netflix.titus.common.runtime.TitusRuntimes;
import com.netflix.titus.common.util.archaius2.Archaius2Ext;
import com.netflix.titus.common.util.loadshedding.AdaptiveAdmissionController.ErrorKind;
import org.junit.Test;

import static com.jayway.awaitility.Awaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

public class SimpleAdmissionBackoffStrategyTest {

    private final TitusRuntime titusRuntime = TitusRuntimes.internal();

    @Test
    public void testBackoffRateLimited() {
        SimpleAdmissionBackoffStrategy backoff = new SimpleAdmissionBackoffStrategy("test", newConfiguration(), titusRuntime);
        assertThat(backoff.getThrottleFactor()).isEqualTo(1.0);

        // Unavailable
        backoff.onError(1, ErrorKind.RateLimited, null);
        await().until(() -> backoff.getThrottleFactor() <= 0.9);

        // Recovery
        testRecovery(backoff);
    }

    @Test
    public void testBackoffUnavailable() {
        SimpleAdmissionBackoffStrategy backoff = new SimpleAdmissionBackoffStrategy("test", newConfiguration(), titusRuntime);
        assertThat(backoff.getThrottleFactor()).isEqualTo(1.0);

        // Unavailable
        backoff.onError(1, ErrorKind.Unavailable, null);
        await().until(() -> backoff.getThrottleFactor() == 0.1);

        // Recovery
        testRecovery(backoff);
    }

    private void testRecovery(SimpleAdmissionBackoffStrategy backoff) {
        double factor = backoff.getThrottleFactor();
        while (factor < 1.0) {
            backoff.onSuccess(1);
            double finalFactor = factor;
            await().pollDelay(1, TimeUnit.MILLISECONDS).until(() -> backoff.getThrottleFactor() > finalFactor);
            factor = backoff.getThrottleFactor();
        }
    }

    private SimpleAdmissionBackoffStrategyConfiguration newConfiguration() {
        return Archaius2Ext.newConfiguration(SimpleAdmissionBackoffStrategyConfiguration.class,
                "monitoringIntervalMs", "1"
        );
    }
}