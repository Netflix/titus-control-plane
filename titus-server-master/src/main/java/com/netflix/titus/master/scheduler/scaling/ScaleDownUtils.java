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

package com.netflix.titus.master.scheduler.scaling;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.netflix.fenzo.VirtualMachineLease;

/**
 */
public class ScaleDownUtils {

    public static String toCompactString(List<Set<VirtualMachineLease>> result) {
        List<Set<String>> hostGroups = result.stream()
                .map(g -> g.stream().map(VirtualMachineLease::hostname).collect(Collectors.toSet()))
                .collect(Collectors.toList());

        return "{groups=" + hostGroups + '}';
    }
}
