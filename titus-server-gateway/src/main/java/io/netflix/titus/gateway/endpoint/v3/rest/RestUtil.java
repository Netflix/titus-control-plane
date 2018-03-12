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

package io.netflix.titus.gateway.endpoint.v3.rest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.ws.rs.core.MultivaluedMap;

import com.netflix.titus.grpc.protogen.Page;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.StringExt;

import static io.netflix.titus.common.util.CollectionsExt.asSet;

final class RestUtil {

    static final Set<String> IGNORED_QUERY_PARAMS = asSet(
            "debug", "fields", "page", "pagesize", "cursor", "accesstoken"
    );

    static Page createPage(MultivaluedMap<String, String> map) {
        Page.Builder pageBuilder = Page.newBuilder();
        pageBuilder.setPageNumber(Integer.parseInt(getFirstOrDefault(map, "page", "0")));
        pageBuilder.setPageSize(Integer.parseInt(getFirstOrDefault(map, "pageSize", "10")));
        pageBuilder.setCursor(getFirstOrDefault(map, "cursor", ""));
        return pageBuilder.build();
    }

    static String getFirstOrDefault(MultivaluedMap<String, String> map, String key, String defaultValue) {
        String first = map.getFirst(key);
        if (first == null) {
            return defaultValue;
        }
        return first;
    }

    static Map<String, String> getFilteringCriteria(MultivaluedMap<String, String> map) {
        Map<String, String> filterCriteria = new HashMap<>();
        map.keySet()
                .stream()
                .filter(e -> !RestUtil.IGNORED_QUERY_PARAMS.contains(e.toLowerCase()))
                .forEach(e -> {
                    String first = map.getFirst(e);
                    if (first != null) {
                        filterCriteria.put(e, first);
                    }
                });
        return filterCriteria;
    }

    static List<String> getFieldsParameter(MultivaluedMap<String, String> queryParameters) {
        List<String> fields = queryParameters.get("fields");
        if (CollectionsExt.isNullOrEmpty(fields)) {
            return Collections.emptyList();
        }
        return fields.stream().flatMap(f -> StringExt.splitByComma(f).stream()).collect(Collectors.toList());
    }
}
