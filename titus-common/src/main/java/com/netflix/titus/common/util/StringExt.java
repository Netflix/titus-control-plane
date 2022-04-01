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

package com.netflix.titus.common.util;

import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import com.netflix.titus.common.util.tuple.Either;

import static java.util.Arrays.asList;

/**
 * A set of string manipulation related functions.
 */
public final class StringExt {

    public final static Pattern COMMA_SPLIT_RE = Pattern.compile("\\s*,\\s*");
    public final static Pattern DOT_SPLIT_RE = Pattern.compile("\\s*\\.\\s*");
    public final static Pattern SEMICOLON_SPLIT_RE = Pattern.compile("\\s*;\\s*");
    public final static Pattern COLON_SPLIT_RE = Pattern.compile("\\s*:\\s*");
    public final static Pattern EQUAL_SPLIT_RE = Pattern.compile("\\s*=\\s*");

    // For each enum type, contains a map with keys being enum names in lower case, and the values are their
    // corresponding enum values.
    private static ConcurrentMap<Class<? extends Enum>, Map<String, Object>> ENUM_NAMES_MAP = new ConcurrentHashMap<>();

    private StringExt() {
    }

    /**
     * Return true if the string value is not null, and it is not an empty string.
     */
    public static boolean isNotEmpty(String s) {
        return s != null && !s.isEmpty();
    }

    /**
     * Check if the given string is a valid UUID value.
     */
    public static boolean isUUID(String s) {
        try {
            UUID.fromString(s);
        } catch (Exception ignore) {
            return false;
        }
        return true;
    }

    /**
     * Return true if the string value is null or an empty string.
     */
    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }

    /**
     * Return string with trimmed whitespace characters. If the argument is null, return empty string.
     */
    public static String safeTrim(String s) {
        if (s == null || s.isEmpty()) {
            return "";
        }
        String trimmed = s.trim();
        return trimmed.isEmpty() ? "" : trimmed;
    }

    public static String nonNull(String value) {
        return value == null ? "" : value;
    }

    /**
     * Return prefix until first occurrence of the end marker.
     */
    public static String takeUntil(String value, String endMarker) {
        if (value == null) {
            return "";
        }
        if (endMarker == null) {
            return value;
        }
        if (value.length() < endMarker.length()) {
            return value;
        }
        int idx = value.indexOf(endMarker);
        if (idx < 0) {
            return value;
        }
        return value.substring(0, idx);
    }

    /**
     * Pass a string value to the consumer if it is not null, and non-empty.
     */
    public static void applyIfNonEmpty(String value, Consumer<String> consumer) {
        if (value != null) {
            if (!value.isEmpty()) {
                consumer.accept(value);
            }
        }
    }

    /**
     * Execute an action if the argument is an empty string.
     */
    public static void runIfEmpty(String value, Runnable action) {
        if (isEmpty(value)) {
            action.run();
        }
    }

    /**
     * Pass a trimmed string value to the consumer if it is not null, and non-empty.
     */
    public static void trimAndApplyIfNonEmpty(String value, Consumer<String> consumer) {
        if (value != null) {
            String trimmed = value.trim();
            if (!trimmed.isEmpty()) {
                consumer.accept(trimmed);
            }
        }
    }

    /**
     * Concatenate strings from the given string collection, separating the items with the given delimiter.
     */
    public static String concatenate(Collection<String> stringCollection, String delimiter) {
        if (stringCollection == null) {
            return null;
        }
        Iterator<String> it = stringCollection.iterator();
        if (!it.hasNext()) {
            return "";
        }
        StringBuilder sb = new StringBuilder(it.next());
        while (it.hasNext()) {
            sb.append(delimiter);
            sb.append(it.next());
        }
        return sb.toString();
    }

    /**
     * Concatenate object values converted to string using provided mapping function, separating the items with the given delimiter.
     */
    public static <T> String concatenate(T[] array, String delimiter, Function<T, String> mapping) {
        if (array == null) {
            return null;
        }
        if (array.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(mapping.apply(array[0]));
        for (int i = 1; i < array.length; i++) {
            sb.append(delimiter);
            sb.append(mapping.apply(array[i]));
        }
        return sb.toString();
    }

    /**
     * Concatenate object values converted to string, separating the items with the given delimiter.
     */
    public static String concatenate(Object[] array, String delimiter) {
        return concatenate(array, delimiter, Object::toString);
    }

    /**
     * Concatenate enum values.
     */
    public static <E extends Enum> String concatenate(E[] array, String delimiter) {
        return concatenate(array, delimiter, Enum::name);
    }

    /**
     * Concatenate enum values.
     */
    public static <E extends Enum> String concatenate(Class<E> enumType, String delimiter, Function<E, Boolean> filter) {
        StringBuilder sb = new StringBuilder();
        for (E value : enumType.getEnumConstants()) {
            if (filter.apply(value)) {
                sb.append(value).append(delimiter);
            }
        }
        if (sb.length() == 0) {
            return "";
        }
        sb.setLength(sb.length() - delimiter.length());
        return sb.toString();
    }

    public static String getNonEmptyOrDefault(String value, String defaultValue) {
        return isNotEmpty(value) ? value : defaultValue;
    }

    /**
     * Given sequence of string values, return the first one that is not empty (see {@link #isNotEmpty(String)} or
     * {@link Optional#empty()}.
     */
    public static Optional<String> firstNonEmpty(String... values) {
        for (String v : values) {
            if (isNotEmpty(v)) {
                return Optional.of(v);
            }
        }
        return Optional.empty();
    }

    /**
     * Trim list values, or remove them if a value is an empty string or null.
     */
    public static List<String> trim(List<String> list) {
        List<String> result = new ArrayList<>();
        for (String value : list) {
            if (isNotEmpty(value)) {
                String trimmed = value.trim();
                if (trimmed.length() > 0) {
                    result.add(trimmed);
                }
            }
        }
        return result;
    }

    /**
     * Returns pattern for splitting string by ',' separator optionally surrounded by spaces.
     */
    public static Pattern getCommaSeparator() {
        return COMMA_SPLIT_RE;
    }

    /**
     * Returns pattern for splitting string by '.' separator optionally surrounded by spaces.
     */
    public static Pattern getDotSeparator() {
        return DOT_SPLIT_RE;
    }

    /**
     * Returns pattern for splitting string by ';' separator optionally surrounded by spaces.
     */
    public static Pattern getSemicolonSeparator() {
        return SEMICOLON_SPLIT_RE;
    }

    /**
     * Returns a list of comma separated values from the parameter. The white space characters around each value
     * is removed as well.
     */
    public static List<String> splitByComma(String value) {
        if (!isNotEmpty(value)) {
            return Collections.emptyList();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Collections.emptyList() : asList(COMMA_SPLIT_RE.split(trimmed));
    }

    /**
     * See {@link #splitByComma(String)}.
     */
    public static Set<String> splitByCommaIntoSet(String value) {
        if (!isNotEmpty(value)) {
            return Collections.emptySet();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Collections.emptySet() : CollectionsExt.asSet(COMMA_SPLIT_RE.split(trimmed));
    }

    /**
     * Returns a list of dot separated values from the parameter. The white space characters around each value
     * is removed as well.
     */
    public static List<String> splitByDot(String value) {
        if (!isNotEmpty(value)) {
            return Collections.emptyList();
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? Collections.emptyList() : asList(DOT_SPLIT_RE.split(trimmed));
    }

    /**
     * Parse enum name ignoring case.
     */
    public static <E extends Enum<E>> E parseEnumIgnoreCase(String enumName, Class<E> enumType) {
        String trimmed = safeTrim(enumName);
        if (isEmpty(trimmed)) {
            throw new IllegalArgumentException("Empty string passed as enum name");
        }
        Map<String, Object> enumLowerCaseNameMap = getEnumLowerCaseNameMap(enumType);
        E result = (E) enumLowerCaseNameMap.get(trimmed.toLowerCase());
        if (result == null) {
            throw new IllegalArgumentException("Invalid enum value " + trimmed);
        }
        return result;
    }

    /**
     * Parse a comma separated list of enum values.
     *
     * @throws IllegalArgumentException if the passed names do not represent valid enum values
     */
    public static <E extends Enum<E>> List<E> parseEnumListIgnoreCase(String enumNames, Class<E> enumType) {
        return parseEnumListIgnoreCase(enumNames, enumType, null);
    }

    /**
     * Parse a comma separated list of enum values. A name can denote a set of values, that is replaced during parsing process.
     * Argument to 'groupings' function is in lower case.
     *
     * @throws IllegalArgumentException if the passed names do not represent valid enum values
     */
    public static <E extends Enum<E>> List<E> parseEnumListIgnoreCase(String enumNames, Class<E> enumType, Function<String, List<E>> groupings) {
        if (isEmpty(enumNames)) {
            return Collections.emptyList();
        }
        List<String> names = splitByComma(enumNames);
        if (names.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, Object> enumLowerCaseNameMap = getEnumLowerCaseNameMap(enumType);
        List<E> result = new ArrayList<>();
        for (String name : names) {
            String lowerCaseName = name.toLowerCase();
            E enumName = (E) enumLowerCaseNameMap.get(lowerCaseName);
            if (enumName == null && groupings != null) {
                List<E> valueSet = groupings.apply(lowerCaseName);
                if (valueSet != null) {
                    result.addAll(valueSet);
                    continue;
                }
            }
            if (enumName == null) {
                throw new IllegalArgumentException("Invalid enum value " + name);
            }
            result.add(enumName);
        }
        return result;
    }

    /**
     * Given a text value in format "key1=value1 &lt;separator&gt; key2=value2", convert it to a map.
     */
    public static <A> Map<String, A> parseKeyValueList(String text, Pattern entrySeparator, Pattern pairSeparator,
                                                       BiFunction<A, String, A> accumulator) {
        String trimmed = safeTrim(text);
        if (isEmpty(trimmed)) {
            return Collections.emptyMap();
        }
        List<String> keyValuePairs = asList(entrySeparator.split(trimmed));
        if (keyValuePairs.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, A> result = new HashMap<>();
        for (String keyValuePair : keyValuePairs) {
            int idx = indexOf(pairSeparator, keyValuePair);
            String key;
            String value;
            if (idx == -1) {
                key = keyValuePair;
                value = "";
            } else if (idx == keyValuePair.length() - 1) {
                key = keyValuePair.substring(0, idx);
                value = "";
            } else {
                key = keyValuePair.substring(0, idx);
                value = keyValuePair.substring(idx + 1);
            }

            result.put(key, accumulator.apply(result.get(key), value));
        }
        return result;
    }

    /**
     * Given a text value in format "key1:value1,key2:value2", convert it to a map. If there are multiple values
     * for the same key, the last one wins.
     */
    public static Map<String, String> parseKeyValueList(String text) {
        return parseKeyValueList(text, COMMA_SPLIT_RE, COLON_SPLIT_RE, (a, v) -> v);
    }

    /**
     * Given a text value in format "key1:value1,key2:value2", convert it to a map.
     */
    public static Map<String, Set<String>> parseKeyValuesList(String text) {
        return parseKeyValueList(text, COMMA_SPLIT_RE, COLON_SPLIT_RE, (a, v) -> {
            if (a == null) {
                a = new HashSet<>();
            }
            a.add(v);
            return a;
        });
    }

    private static <E extends Enum<E>> Map<String, Object> getEnumLowerCaseNameMap(Class<E> enumType) {
        Map<String, Object> mapping = ENUM_NAMES_MAP.get(enumType);
        if (mapping != null) {
            return mapping;
        }

        mapping = new HashMap<>();
        for (E value : enumType.getEnumConstants()) {
            mapping.put(value.name().toLowerCase(), value);
        }
        return mapping;
    }

    public static boolean isAsciiLetter(char c) {
        return (c > 64 && c < 91) || (c > 96 && c < 123);
    }

    public static boolean isAsciiDigit(char c) {
        return c >= 48 && c <= 57;
    }

    public static boolean isAsciiLetterOrDigit(char c) {
        return isAsciiLetter(c) || isAsciiDigit(c);
    }

    public static String removeNonAlphanumeric(String text) {
        StringBuilder sb = new StringBuilder();
        for (char c : text.toCharArray()) {
            if (StringExt.isAsciiLetterOrDigit(c)) {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static int indexOf(Pattern pattern, String text) {
        Matcher matcher = pattern.matcher(text);
        return matcher.find() ? matcher.start() : -1;
    }

    public static String doubleQuotes(String text) {
        return "\"" + text + "\"";
    }

    /**
     * Removes quotes (single or double) around the provided text. If a quote is provided on the left or right side
     * only it is not removed. For example "abc" becomes abc, but "abc is left as "abc.
     */
    public static String removeSurroundingQuotes(String text) {
        if (isEmpty(text) || text.length() < 2) {
            return text;
        }
        char first = text.charAt(0);
        char last = text.charAt(text.length() - 1);
        if (first != last) {
            return text;
        }
        if (first == '"' || first == '\'') {
            return text.substring(1, text.length() - 1);
        }
        return text;
    }

    /**
     * Append the value to the end of the text if the value is not already there.
     */
    public static String appendToEndIfMissing(String text, String value) {
        return text.endsWith(value) ? text : text + value;
    }

    public static String startWithLowercase(String text) {
        if (text == null || text.length() == 0 || !Character.isUpperCase(text.charAt(0))) {
            return text;
        }
        return Character.toLowerCase(text.charAt(0)) + text.substring(1);
    }

    public static Optional<Integer> parseInt(String s) {
        if (StringExt.isEmpty(s)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static Optional<Long> parseLong(String s) {
        if (StringExt.isEmpty(s)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Long.parseLong(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    public static Optional<Double> parseDouble(String s) {
        if (StringExt.isEmpty(s)) {
            return Optional.empty();
        }
        try {
            return Optional.of(Double.parseDouble(s));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    /**
     * Parse string value like "1, 10, 1000" to an array of {@link Duration} values, assuming the values represent
     * milliseconds.
     *
     * @returns {@link Optional#empty()} ()} if string is empty or cannot be parsed, and {@link Duration} list otherwise.
     */
    public static Optional<List<Duration>> parseDurationMsList(String s) {
        String list = StringExt.safeTrim(s);
        if (list.isEmpty()) {
            return Optional.empty();
        }
        String[] parts = list.split(",");
        List<Duration> result = new ArrayList<>(parts.length);
        for (String part : parts) {
            try {
                result.add(Duration.ofMillis(Integer.parseInt(part.trim())));
            } catch (Exception e) {
                return Optional.empty();
            }
        }
        return Optional.of(result);
    }

    /**
     * GZip a string and base64 encode the result in order to compress a string while keeping the String type.
     *
     * @return gzipped and base64 encoded string.
     */
    public static String gzipAndBase64Encode(String s) {
        if (StringExt.isEmpty(s)) {
            return s;
        }
        try (
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                GZIPOutputStream gzip = new GZIPOutputStream(out);
        ) {
            gzip.write(s.getBytes());
            gzip.finish();
            return Base64.getEncoder().encodeToString(out.toByteArray());
        } catch (Exception e) {
            throw ExceptionExt.rethrow(e);
        }
    }

    public static Either<String, IllegalArgumentException> nameFromJavaBeanGetter(String getterName) {
        if (isEmpty(getterName)) {
            return Either.ofError(new IllegalArgumentException("getter name is empty"));
        }
        int prefixLen;
        if (getterName.startsWith("get")) {
            prefixLen = 3;
        } else if (getterName.startsWith("is")) {
            prefixLen = 2;
        } else if (getterName.startsWith("has")) {
            prefixLen = 3;
        } else {
            return Either.ofError(new IllegalArgumentException(String.format("getter '%s' does not start with a valid prefix (get|is|has)", getterName)));
        }

        if (getterName.length() == prefixLen) {
            return Either.ofError(new IllegalArgumentException(String.format("getter '%s' has only prefix with empty base name", getterName)));
        }

        return Either.ofValue(Character.toLowerCase(getterName.charAt(prefixLen)) + getterName.substring(prefixLen + 1));
    }
}