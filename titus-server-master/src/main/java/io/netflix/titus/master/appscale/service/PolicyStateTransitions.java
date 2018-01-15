package io.netflix.titus.master.appscale.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.netflix.titus.api.appscale.model.PolicyStatus;

public class PolicyStateTransitions {
    private static Map<PolicyStatus, List<PolicyStatus>> allowedTransitions = new HashMap<PolicyStatus, List<PolicyStatus>>() {{
        put(PolicyStatus.Applied, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Pending);
            add(PolicyStatus.Deleting);
        }});

        put(PolicyStatus.Pending, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Applied);
            add(PolicyStatus.Error);
        }});

        put(PolicyStatus.Deleting, new ArrayList<PolicyStatus>() {{
            add(PolicyStatus.Deleted);
            add(PolicyStatus.Error);
        }});
        put(PolicyStatus.Deleted, Collections.emptyList());
        put(PolicyStatus.Error, Collections.emptyList());
    }};

    public static boolean isAllowed(PolicyStatus from, PolicyStatus to) {
        return allowedTransitions.containsKey(from) && allowedTransitions.get(from).contains(to);
    }
}
