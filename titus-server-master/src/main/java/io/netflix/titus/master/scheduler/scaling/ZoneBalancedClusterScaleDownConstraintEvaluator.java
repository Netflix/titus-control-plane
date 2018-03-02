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

package io.netflix.titus.master.scheduler.scaling;

import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import javax.inject.Singleton;

import com.netflix.fenzo.ScaleDownConstraintEvaluator;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.BalancedScaleDownConstraintEvaluator;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.scheduler.constraint.LeaseAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ZoneBalancedClusterScaleDownConstraintEvaluator implements ScaleDownConstraintEvaluator<Map<String, Integer>> {

    private static final Logger logger = LoggerFactory.getLogger(ZoneBalancedClusterScaleDownConstraintEvaluator.class);

    private static final String UNKNOWN_ZONE = "unknownZone";

    private static final double INITIAL_SCORE = 0.7;
    private static final double INITIAL_STEP = 0.1;

    private final BalancedScaleDownConstraintEvaluator delegate;
    private final String zoneKeyName;

    @Inject
    public ZoneBalancedClusterScaleDownConstraintEvaluator(MasterConfiguration config) {
        this.delegate = new BalancedScaleDownConstraintEvaluator(this::keyExtractor, INITIAL_SCORE, INITIAL_STEP);
        this.zoneKeyName = config.getHostZoneAttributeName();
    }

    @Override
    public String getName() {
        return ZoneBalancedClusterScaleDownConstraintEvaluator.class.getSimpleName();
    }

    @Override
    public Result<Map<String, Integer>> evaluate(VirtualMachineLease candidate, Optional<Map<String, Integer>> optionalContext) {
        Result<Map<String, Integer>> result = delegate.evaluate(candidate, optionalContext);
        if (logger.isDebugEnabled()) {
            logger.debug("Computed score for host {}: {}", candidate.hostname(), result.getScore());
        }
        return result;
    }

    private String keyExtractor(VirtualMachineLease lease) {
        return LeaseAttributes.getOrDefault(lease, zoneKeyName, UNKNOWN_ZONE);
    }
}
