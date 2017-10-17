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

package io.netflix.titus.common.util.unit;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Preconditions;

public enum MemoryUnit {

    BYTES() {
        @Override
        public long toBytes(long memorySize) {
            return memorySize;
        }

        @Override
        public long toKiloBytes(long memorySize) {
            return memorySize / _1KB;
        }

        @Override
        public long toMegaBytes(long memorySize) {
            return memorySize / _1MB;
        }

        @Override
        public long toGigaBytes(long memorySize) {
            return memorySize / _1GB;
        }

        @Override
        public long convert(long memorySize, MemoryUnit memoryUnit) {
            return memoryUnit.toBytes(memorySize);
        }

        @Override
        public String format(long memorySize) {
            return memorySize + "B";
        }
    },
    KILOBYTES {
        @Override
        public long toBytes(long memorySize) {
            return memorySize * _1KB;
        }

        @Override
        public long toKiloBytes(long memorySize) {
            return memorySize;
        }

        @Override
        public long toMegaBytes(long memorySize) {
            return memorySize / _1KB;
        }

        @Override
        public long toGigaBytes(long memorySize) {
            return memorySize / _1MB;
        }

        @Override
        public long convert(long memorySize, MemoryUnit memoryUnit) {
            return memoryUnit.toKiloBytes(memorySize);
        }

        @Override
        public String format(long memorySize) {
            return memorySize + "KB";
        }
    },
    MEGABYTES {
        @Override
        public long toBytes(long memorySize) {
            return memorySize * _1MB;
        }

        @Override
        public long toKiloBytes(long memorySize) {
            return memorySize * _1KB;
        }

        @Override
        public long toMegaBytes(long memorySize) {
            return memorySize;
        }

        @Override
        public long toGigaBytes(long memorySize) {
            return memorySize / _1KB;
        }

        @Override
        public long convert(long memorySize, MemoryUnit memoryUnit) {
            return memoryUnit.toMegaBytes(memorySize);
        }

        @Override
        public String format(long memorySize) {
            return memorySize + "MB";
        }
    },
    GIGABYTES {
        @Override
        public long toBytes(long memorySize) {
            return memorySize * _1GB;
        }

        @Override
        public long toKiloBytes(long memorySize) {
            return memorySize * _1MB;
        }

        @Override
        public long toMegaBytes(long memorySize) {
            return memorySize * _1KB;
        }

        @Override
        public long toGigaBytes(long memorySize) {
            return memorySize;
        }

        @Override
        public long convert(long memorySize, MemoryUnit memoryUnit) {
            return memoryUnit.toGigaBytes(memorySize);
        }

        @Override
        public String format(long memorySize) {
            return memorySize + "GB";
        }
    };

    static final long _1KB = 1024;
    static final long _1MB = _1KB * _1KB;
    static final long _1GB = _1MB * _1KB;

    private static final Pattern BYTES_RE = Pattern.compile("\\s*(\\d+)\\s*(B|Bytes)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern KB_RE = Pattern.compile("\\s*(\\d+)\\s*(K|KB|KBytes|KiloBytes)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern MB_RE = Pattern.compile("\\s*(\\d+)\\s*(M|MB|MBytes|MegaBytes)\\s*", Pattern.CASE_INSENSITIVE);
    private static final Pattern GB_RE = Pattern.compile("\\s*(\\d+)\\s*(G|GB|GBytes|GigaBytes)\\s*", Pattern.CASE_INSENSITIVE);

    public long toBytes(long memorySize) {
        throw new AbstractMethodError();
    }

    public long toKiloBytes(long memorySize) {
        throw new AbstractMethodError();
    }

    public long toMegaBytes(long memorySize) {
        throw new AbstractMethodError();
    }

    public long toGigaBytes(long memorySize) {
        throw new AbstractMethodError();
    }

    public long convert(long memorySize, MemoryUnit memoryUnit) {
        throw new AbstractMethodError();
    }

    public String format(long memorySize) {
        throw new AbstractMethodError();
    }

    public static long bytesOf(String memorySize) {
        Preconditions.checkNotNull(memorySize);

        Matcher bytesMatcher = BYTES_RE.matcher(memorySize);
        if (bytesMatcher.matches()) {
            return Long.parseLong(bytesMatcher.group(1));
        }

        Matcher kbMatcher = KB_RE.matcher(memorySize);
        if (kbMatcher.matches()) {
            return KILOBYTES.toBytes(Long.parseLong(kbMatcher.group(1)));
        }

        Matcher mbMatcher = MB_RE.matcher(memorySize);
        if (mbMatcher.matches()) {
            return MEGABYTES.toBytes(Long.parseLong(mbMatcher.group(1)));
        }

        Matcher gbMatcher = GB_RE.matcher(memorySize);
        if (gbMatcher.matches()) {
            return GIGABYTES.toBytes(Long.parseLong(gbMatcher.group(1)));
        }

        throw new IllegalArgumentException("Invalid memory size format: " + memorySize);
    }

    public static long kiloBytesOf(String memorySize) {
        return BYTES.toKiloBytes(bytesOf(memorySize));
    }

    public static long megaBytesOf(String memorySize) {
        return BYTES.toMegaBytes(bytesOf(memorySize));
    }

    public static long gigaBytesOf(String memorySize) {
        return BYTES.toGigaBytes(bytesOf(memorySize));
    }
}
