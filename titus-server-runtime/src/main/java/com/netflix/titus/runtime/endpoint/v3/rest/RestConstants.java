package com.netflix.titus.runtime.endpoint.v3.rest;

import java.util.Set;

import static com.netflix.titus.common.util.CollectionsExt.asSet;

public class RestConstants {
    static final Set<String> IGNORED_QUERY_PARAMS = asSet(
            "debug", "fields", "page", "pagesize", "cursor", "accesstoken"
    );

    public static final String PAGE_QUERY_KEY = "page";
    public static final String PAGE_SIZE_QUERY_KEY = "pageSize";
    public static final String CURSOR_QUERY_KEY = "cursor";
    public static final String FIELDS_QUERY_KEY = "fields";
}
