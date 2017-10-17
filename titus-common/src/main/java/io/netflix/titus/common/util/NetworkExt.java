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

package io.netflix.titus.common.util;

import java.io.InputStream;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of network related functions.
 */
public class NetworkExt {

    private static final Logger logger = LoggerFactory.getLogger(NetworkExt.class);

    public static final String IP4_LOOPBACK = "127.0.0.1";
    public static final String IP6_LOOPBACK = "0:0:0:0:0:0:0:1";

    private static final AtomicReference<Optional<String>> resolvedHostNameRef = new AtomicReference<>();
    private static final AtomicReference<Optional<List<String>>> resolvedLocalIPsRef = new AtomicReference<>();

    /**
     * Return resolved local host name. The value is cached, so actual resolution happens only during the first call.
     *
     * @return host name or {@link Optional#empty()} if resolution failed.
     */
    public static Optional<String> getHostName() {
        if (resolvedHostNameRef.get() == null) {
            try {
                resolvedHostNameRef.set(Optional.of(resolveHostName()));
            } catch (Exception e) {
                logger.error("Cannot resolve local host name");
                resolvedHostNameRef.set(Optional.empty());
            }
        }
        return resolvedHostNameRef.get();
    }

    /**
     * Resolve host name. No caching mechanism is implemented in this method, so these calls are expensive.
     * Use {@link #getHostName()} for faster operation.
     *
     * @return host name
     * @throws RuntimeException if host name resolution fails
     */
    public static String resolveHostName() {
        // Try first hostname system call, to avoid potential
        // issues with InetAddress.getLocalHost().getHostName()
        try {
            Process process = Runtime.getRuntime().exec("hostname");
            InputStream input = process.getInputStream();
            StringBuilder sb = new StringBuilder();
            byte[] bytes = new byte[256];
            int len;
            while ((len = input.read(bytes)) != -1) {
                sb.append(new String(bytes, 0, len, Charset.defaultCharset()));
            }
            if (process.exitValue() == 0) {
                return sb.toString().trim();
            }
        } catch (Exception ignore) {
            // Ignore this exception, we will try different way
        }

        // Try InetAddress.getLocalHost().getHostName()
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException("cannot resolve local host name", e);
        }
    }

    /**
     * Returns true if the given IP is a loopback address.
     */
    public static boolean isLoopbackIP(String ip) {
        return ip.equals(IP4_LOOPBACK) || ip.startsWith(IP6_LOOPBACK);
    }

    /**
     * Returns true if the given IP is IPv6.
     */
    public static boolean isIPv6(String ip) {
        return ip.indexOf(':') != -1;
    }

    /**
     * Return resolved local IP addresses. The value is cached, so actual resolution happens only during the first call.
     *
     * @return IP address list or {@link Optional#empty()} if resolution failed.
     */
    public static Optional<List<String>> getLocalIPs() {
        if (resolvedLocalIPsRef.get() == null) {
            try {
                resolvedLocalIPsRef.set(Optional.of(resolveLocalIPs()));
            } catch (Exception e) {
                logger.error("Cannot resolve local IP addresses");
                resolvedLocalIPsRef.set(Optional.empty());
            }
        }
        return resolvedLocalIPsRef.get();
    }

    /**
     * Returns all local IP addresses (IPv4 and IPv6).
     *
     * @throws RuntimeException if resolution fails
     */
    public static List<String> resolveLocalIPs() {
        ArrayList<String> addresses = new ArrayList<>();
        try {
            Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces();
            while (nics.hasMoreElements()) {
                NetworkInterface nic = nics.nextElement();
                Enumeration<InetAddress> inetAddresses = nic.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress address = inetAddresses.nextElement();
                    addresses.add(address.getHostAddress());
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException("Cannot resolve local network addresses", e);
        }
        return Collections.unmodifiableList(addresses);
    }

    public static String toIPv4(long addressLong) {
        return Long.toString(addressLong >> 24 & 0xFF) + '.' +
                Long.toString(addressLong >> 16 & 0xFF) + '.' +
                Long.toString(addressLong >> 8 & 0xFF) + '.' +
                Long.toString(addressLong & 0xFF);
    }

    public static long toNetworkBitMask(int maskLength) {
        Preconditions.checkArgument(maskLength > 0 && maskLength <= 32, "Mask length must be in (0, 32> range: " + maskLength);
        long subnet = 0;
        for (int i = 0; i < maskLength; i++) {
            subnet = (subnet << 1) | 1;
        }
        return 0xFFFFFFFFL ^ subnet;
    }

    public static boolean matches(String ipAddress, NetworkAddress networkAddress) {
        return matches(toBits(ipAddress), networkAddress);
    }

    public static boolean matches(long ipAddress, NetworkAddress networkAddress) {
        return (ipAddress & networkAddress.getMask()) == networkAddress.getAddressLong();
    }

    public static Function<String, Boolean> buildNetworkMatchPredicate(List<String> cdirs) {
        List<NetworkAddress> networkAddresses = cdirs.stream().map(NetworkExt::parseCDIR).collect(Collectors.toList());
        return address -> {
            if (!isIpV4(address)) {
                return false;
            }
            long addressBits = toBits(address);
            for (NetworkAddress networkAddress : networkAddresses) {
                if (matches(addressBits, networkAddress)) {
                    return true;
                }
            }
            return false;
        };
    }

    public static NetworkAddress parseCDIR(String cdir) {
        Matcher matcher = IPV4_CDIR_RE.matcher(cdir);
        Preconditions.checkArgument(matcher.matches(), "Expected network address in CDIR format, but got " + cdir);
        return new NetworkAddress(matcher.group(1), Integer.parseInt(matcher.group(2)));
    }

    private static final Pattern IPV4_CDIR_RE = Pattern.compile("(\\d+[.]\\d+[.]\\d+[.]\\d+)/(\\d+)");
    private static final Pattern IPV4_RE = Pattern.compile("(\\d+)[.](\\d+)[.](\\d+)[.](\\d+)");

    /**
     * Limited to IPv4.
     */
    public static class NetworkAddress {
        private final String address;
        private final int maskLength;
        private final long mask;
        private final long addressLong;

        private NetworkAddress(String address, int maskLength) {
            this.address = address;
            this.maskLength = maskLength;
            this.mask = toNetworkBitMask(maskLength);
            this.addressLong = toBits(address) & mask;
        }

        public String getAddress() {
            return address;
        }

        public int getMaskLength() {
            return maskLength;
        }

        public long getMask() {
            return mask;
        }

        public long getAddressLong() {
            return addressLong;
        }
    }

    public static boolean isIpV4(String address) {
        return parseIpV4(address).isPresent();
    }

    public static long toBits(String ipv4Address) {
        Optional<int[]> parsedOpt = parseIpV4(ipv4Address);
        if (!parsedOpt.isPresent()) {
            throw new IllegalArgumentException("Not an IPv4 address: " + ipv4Address);
        }
        int[] bytes = parsedOpt.get();
        return ((bytes[3] << 8 | bytes[2]) << 8 | bytes[1]) << 8 | bytes[0];
    }

    private static Optional<int[]> parseIpV4(String address) {
        Matcher matcher = IPV4_RE.matcher(address);
        if (!matcher.matches()) {
            return Optional.empty();
        }
        int b3 = Integer.parseInt(matcher.group(1));
        int b2 = Integer.parseInt(matcher.group(2));
        int b1 = Integer.parseInt(matcher.group(3));
        int b0 = Integer.parseInt(matcher.group(4));
        if (b3 < 0 || b3 > 255 || b2 < 0 || b2 > 255 | b1 < 0 || b1 > 255 || b0 < 0 || b0 > 255) {
            return Optional.empty();
        }
        return Optional.of(new int[]{b0, b1, b2, b3});
    }
}
