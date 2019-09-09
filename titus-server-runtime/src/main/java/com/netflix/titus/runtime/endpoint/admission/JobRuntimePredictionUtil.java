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

package com.netflix.titus.runtime.endpoint.admission;

import org.apache.commons.math3.distribution.NormalDistribution;

public final class JobRuntimePredictionUtil {

    private static final int LOW_QUANTILE = 5;
    private static final int HIGH_QUANTILE = 95;

    static final double NORM_SIGMA = computeNormSigma();

    private JobRuntimePredictionUtil() {
    }

    private static double computeNormSigma() {
        NormalDistribution normal = new NormalDistribution();
        return normal.inverseCumulativeProbability(HIGH_QUANTILE / 100.0) - normal.inverseCumulativeProbability(LOW_QUANTILE / 100.0);
    }
}
