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

package io.netflix.titus.common.util;

public class MathExt {
    /**
     * Scale the value using feature scaling such that the value is normalized between the range of [0.0, 1.0].
     *
     * @param value the value to scale
     * @param min   the min value
     * @param max   the max value
     * @return the scaled value
     */
    public static double scale(double value, double min, double max) {
        return (value - min) / (max - min);
    }

    /**
     * Scale the value using feature scaling such that the value is normalized between the range of [minRangeValue, maxRangeValue].
     *
     * @param value         the value to scale
     * @param min           the min value
     * @param max           the max value
     * @param minRangeValue the min range value
     * @param maxRangeValue the max range value
     * @return the scaled value
     */
    public static double scale(double value, double min, double max, double minRangeValue, double maxRangeValue) {
        return minRangeValue + (((value - min) * (maxRangeValue - minRangeValue)) / (max - min));
    }
}
