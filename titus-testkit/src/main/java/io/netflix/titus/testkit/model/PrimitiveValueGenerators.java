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

package io.netflix.titus.testkit.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import io.netflix.titus.common.data.generator.DataGenerator;
import io.netflix.titus.common.util.NetworkExt;
import io.netflix.titus.common.util.tuple.Pair;

import static io.netflix.titus.common.data.generator.DataGenerator.concatenations;
import static io.netflix.titus.common.data.generator.DataGenerator.items;
import static io.netflix.titus.common.data.generator.DataGenerator.range;
import static io.netflix.titus.common.data.generator.DataGenerator.zip;
import static io.netflix.titus.common.util.CollectionsExt.asSet;
import static io.netflix.titus.common.util.NetworkExt.NetworkAddress;
import static io.netflix.titus.common.util.NetworkExt.parseCDIR;

/**
 * Primitive value generators.
 */
public final class PrimitiveValueGenerators {

    static Set<Character> HEX_DIGITS_SET = asSet(
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
            'A', 'B', 'C', 'D', 'E', 'F'
    );

    static Character[] HEX_DIGITS = HEX_DIGITS_SET.toArray(new Character[16]);

    private PrimitiveValueGenerators() {
    }

    /**
     * Generate sequence of IPv4 addresses within a given CIDR.
     */
    public static DataGenerator<String> ipv4CIDRs(String cidr) {
        NetworkAddress networkAddress = parseCDIR(cidr);
        long networkAddressLong = networkAddress.getAddressLong() & networkAddress.getMask();
        int lastAddress = 1 << networkAddress.getMaskLength();
        return DataGenerator.range(1, lastAddress).map(n -> NetworkExt.toIPv4(networkAddressLong | n));
    }

    /**
     * User names generator.
     */
    public static DataGenerator<String> userNames() {
        return items("john", "robert", "albert", "david", "mark");
    }

    /**
     * Application names generator.
     */
    public static DataGenerator<String> applicationNames() {
        return items("titus", "spinnaker", "eureka", "properties", "cassandra");
    }

    /**
     * Internet domain names generator.
     */
    public static DataGenerator<String> internetDomains() {
        return concatenations(".",
                items("serviceA", "serviceB", "serviceC").loop(PrimitiveValueGenerators::appendIndex),
                items("com", "net", "io")
        );
    }

    /**
     * Email addresses generator.
     */
    public static DataGenerator<String> emailAddresses() {
        return zip(userNames().loop(PrimitiveValueGenerators::appendIndex), internetDomains()).map(pair -> pair.getLeft() + '@' + pair.getRight());
    }

    /**
     * Generates semantic version numbers up to a given depth.
     */
    public static DataGenerator<String> semanticVersions(int depth) {
        List<DataGenerator<String>> numberGenerators = new ArrayList<>();

        numberGenerators.add(range(1, 10).map(n -> Long.toString(n)));

        DataGenerator<String> subVersionGenerator = range(0, 10).map(n -> Long.toString(n));
        for (int i = 1; i < depth; i++) {
            numberGenerators.add(subVersionGenerator);
        }

        return concatenations(".", numberGenerators);
    }

    /**
     * Sequences of hex-base values.
     */
    public static DataGenerator<String> hexValues(int length) {
        return DataGenerator.sequenceOfString(new Random(), length, random -> Pair.of(random, HEX_DIGITS[random.nextInt(16)]));
    }

    private static String appendIndex(String value, int idx) {
        return idx == 0 ? value : value + idx;
    }
}
