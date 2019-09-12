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

import com.netflix.titus.runtime.connector.prediction.JobRuntimePrediction;
import org.apache.commons.math3.distribution.NormalDistribution;

final class JobRuntimePredictionUtil {

    private static final double LOW_QUANTILE = 0.05;
    private static final double HIGH_QUANTILE = 0.95;

    static final double NORM_SIGMA = computeNormSigma();

    private JobRuntimePredictionUtil() {
    }

    static boolean expectedQuantiles(JobRuntimePrediction low, JobRuntimePrediction high) {
        return low.getConfidence() == LOW_QUANTILE && high.getConfidence() == HIGH_QUANTILE;
    }

    /**
     * Estimate the standard deviation of a gaussian distribution given 2 quantiles. See https://www.johndcook.com/quantiles_parameters.pdf
     */
    private static double computeNormSigma() {
        NormalDistribution normal = new NormalDistribution();
        return normal.inverseCumulativeProbability(HIGH_QUANTILE) - normal.inverseCumulativeProbability(LOW_QUANTILE);
    }
}
