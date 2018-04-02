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

package com.netflix.titus.common.util.unit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

public enum BandwidthUnit {

    BPS() {
        @Override
        public long toBps(long rate) {
            return rate;
        }

        @Override
        public long toKbps(long rate) {
            return rate / _1K;
        }

        @Override
        public long toMbps(long rate) {
            return rate / _1M;
        }

        @Override
        public long toGbps(long rate) {
            return rate / _1G;
        }

        @Override
        public long convert(long rate, BandwidthUnit bandwidthUnit) {
            return bandwidthUnit.toBps(rate);
        }

        @Override
        public String format(long rate) {
            return rate + "bps";
        }
    },
    KBPS() {
        @Override
        public long toBps(long rate) {
            return rate * _1K;
        }

        @Override
        public long toKbps(long rate) {
            return rate;
        }

        @Override
        public long toMbps(long rate) {
            return rate / _1K;
        }

        @Override
        public long toGbps(long rate) {
            return rate / _1M;
        }

        @Override
        public long convert(long rate, BandwidthUnit bandwidthUnit) {
            return bandwidthUnit.toKbps(rate);
        }

        @Override
        public String format(long rate) {
            return rate + "kbps";
        }
    },
    MBPS() {
        @Override
        public long toBps(long rate) {
            return rate * _1M;
        }

        @Override
        public long toKbps(long rate) {
            return rate * _1K;
        }

        @Override
        public long toMbps(long rate) {
            return rate;
        }

        @Override
        public long toGbps(long rate) {
            return rate / _1K;
        }

        @Override
        public long convert(long rate, BandwidthUnit bandwidthUnit) {
            return bandwidthUnit.toMbps(rate);
        }

        @Override
        public String format(long rate) {
            return rate + "mbps";
        }
    },
    GBPS {
        @Override
        public long toBps(long rate) {
            return rate * _1G;
        }

        @Override
        public long toKbps(long rate) {
            return rate * _1M;
        }

        @Override
        public long toMbps(long rate) {
            return rate * _1K;
        }

        @Override
        public long toGbps(long rate) {
            return rate;
        }

        @Override
        public long convert(long rate, BandwidthUnit bandwidthUnit) {
            return bandwidthUnit.toGbps(rate);
        }

        @Override
        public String format(long rate) {
            return rate + "gbps";
        }
    };

    public long toBps(long rate) {
        throw new AbstractMethodError();
    }

    public long toKbps(long rate) {
        throw new AbstractMethodError();
    }

    public long toMbps(long rate) {
        throw new AbstractMethodError();
    }

    public long toGbps(long rate) {
        throw new AbstractMethodError();
    }

    public long convert(long rate, BandwidthUnit bandwidthUnit) {
        throw new AbstractMethodError();
    }

    public String format(long rate) {
        throw new AbstractMethodError();
    }

    static final long _1K = 1000;
    static final long _1M = _1K * _1K;
    static final long _1G = _1M * _1K;

    private static final Pattern BPS_RE = Pattern.compile("\\s*(\\d+)\\s*(b|bps)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern KBPS_RE = Pattern.compile("\\s*(\\d+)\\s*(k|kbps)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern MBPS_RE = Pattern.compile("\\s*(\\d+)\\s*(m|mbps)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern GBPS_RE = Pattern.compile("\\s*(\\d+)\\s*(g|gbps)\\s*", Pattern.CASE_INSENSITIVE);

    public static long bpsOf(String rate) {
        Preconditions.checkNotNull(rate);

        Matcher bpsMatcher = BPS_RE.matcher(rate);
        if (bpsMatcher.matches()) {
            return Long.parseLong(bpsMatcher.group(1));
        }

        Matcher kbpsMatcher = KBPS_RE.matcher(rate);
        if (kbpsMatcher.matches()) {
            return KBPS.toBps(Long.parseLong(kbpsMatcher.group(1)));
        }

        Matcher mbpsMatcher = MBPS_RE.matcher(rate);
        if (mbpsMatcher.matches()) {
            return MBPS.toBps(Long.parseLong(mbpsMatcher.group(1)));
        }

        Matcher gbpsMatcher = GBPS_RE.matcher(rate);
        if (gbpsMatcher.matches()) {
            return GBPS.toBps(Long.parseLong(gbpsMatcher.group(1)));
        }

        throw new IllegalArgumentException("Invalid bandwidth format: " + rate);
    }

    public static long kbpsOf(String rate) {
        return BPS.toKbps(bpsOf(rate));
    }

    public static long mbpsOf(String rate) {
        return BPS.toMbps(bpsOf(rate));
    }

    public static long gbpsOf(String rate) {
        return BPS.toGbps(bpsOf(rate));
    }
}
