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
package io.netflix.titus.runtime.loadbalancer;

import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import io.netflix.titus.common.util.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


import io.netflix.titus.api.loadbalancer.model.JobLoadBalancer;

public class LoadBalancerCursors {
    private static final Logger logger = LoggerFactory.getLogger(LoadBalancerCursors.class);

    private static final Pattern CURSOR_FORMAT_RE = Pattern.compile("(.*)@(.*)");

    public static Comparator<JobLoadBalancer> loadBalancerComparator() {
        return (first, second) -> {
            String firstValue = first.getJobId() + '@' + first.getLoadBalancerId();
            String secondValue = second.getJobId() + '@' + second.getLoadBalancerId();
            return firstValue.compareTo(secondValue);
        };
    }

    public static Optional<Integer> loadBalancerIndexOf(List<JobLoadBalancer> sortedLoadBalancers, String cursor) {
        return decode(cursor).map(p -> {
            final JobLoadBalancer jobLoadBalancer = new JobLoadBalancer(p.getLeft(), p.getRight());
            final int idx = Collections.binarySearch(sortedLoadBalancers, jobLoadBalancer, loadBalancerComparator());
            return idx >= 0 ? idx : Math.max(-1, -idx - 2);
        });

    }

    public static String newCursorFrom(JobLoadBalancer jobLoadBalancer) {
        return encode(jobLoadBalancer.getJobId(), jobLoadBalancer.getLoadBalancerId());
    }

    private static String encode(String jobId, String loadBalancerId) {
        String value = jobId + '@' + loadBalancerId;
        return Base64.getEncoder().encodeToString(value.getBytes());
    }

    private static Optional<Pair<String, String>> decode(String encodedValue) {
        String decoded;
        try {
            decoded = new String(Base64.getDecoder().decode(encodedValue.getBytes()));
        } catch (Exception e) {
            logger.debug("Cannot decode value: {}", encodedValue, e);
            return Optional.empty();
        }

        Matcher matcher = CURSOR_FORMAT_RE.matcher(decoded);
        if (!matcher.matches()) {
            logger.debug("Not valid cursor value: {}", decoded);
            return Optional.empty();
        }
        return Optional.of(Pair.of(matcher.group(1), matcher.group(2)));
    }
}
