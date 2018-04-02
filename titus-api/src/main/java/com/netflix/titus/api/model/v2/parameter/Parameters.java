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

package com.netflix.titus.api.model.v2.parameter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.MigrationPolicy;
import com.netflix.titus.api.model.EfsMount;
import com.netflix.titus.api.model.MigrationPolicy;

import static java.util.Arrays.asList;

/**
 * Well known parameters directly manipulated in runtime/store layers.
 */
public final class Parameters {

    public enum JobType {Service, Batch}

    public static final String CMD_TYPE = "type";

    public static final String IMAGE_NAME = "imageName";
    public static final String IMAGE_DIGEST = "imageDigest";
    public static final String VERSION = "version";

    public static final String NAME = "name";
    public static final String APP_NAME = "appName";
    public static final String JOB_TYPE = "jobType";
    public static final String IAM_PROFILE = "iamProfile";

    public static final String LABELS = "labels";

    public static final String JOB_GROUP_STACK = "jobGroupStack";
    public static final String JOB_GROUP_DETAIL = "jobGroupDetail";
    public static final String JOB_GROUP_SEQ = "jobGroupSeq";
    public static final String JOB_CAPACITY_GROUP = "capacityGroup";
    public static final String JOB_MIGRATION_POLICY = "migrationPolicy";
    public static final String JOB_ID_SEQUENCE = "jobIdSequence";

    public static final String REQUESTED_PORTS = "requestedPorts";

    public static final String ENTRY_POINT = "entryPoint";
    public static final String SECURITY_GROUPS = "securityGroups";
    public static final String EFS = "efs";
    public static final String EFS_MOUNTS = "efsMounts";
    public static final String IN_SERVICE = "inService";
    public static final String ENV = "env";

    public static final String RESTART_ON_SUCESS = "restartOnSuccess";

    private static final Pattern COMA_SPLIT_RE = Pattern.compile(",");

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final TypeReference<List<EfsMount>> EFS_MOUNTS_TYPE_REF = new TypeReference<List<EfsMount>>() {
    };

    private static final TypeReference<Map<String, String>> MAP_TYPE_REF = new TypeReference<Map<String, String>>() {
    };

    private Parameters() {
    }

    public static String getCmdType(List<Parameter> parameters) {
        return findParameter(parameters, CMD_TYPE);
    }

    public static Parameter newCmdTypeParameter(String cmd) {
        return new Parameter(CMD_TYPE, cmd);
    }

    public static String getName(List<Parameter> parameters) {
        return findParameter(parameters, NAME);
    }

    public static Parameter newNameParameter(String name) {
        return new Parameter(NAME, name);
    }

    public static String getAppName(List<Parameter> parameters) {
        return findParameter(parameters, APP_NAME);
    }

    public static Parameter newAppNameParameter(String appName) {
        return new Parameter(APP_NAME, appName);
    }

    public static String getImageDigest(List<Parameter> parameters) {
        return findParameter(parameters, IMAGE_DIGEST);
    }

    public static Parameter newImageDigestParameter(String imageDigest) {
        return new Parameter(IMAGE_DIGEST, imageDigest);
    }

    public static String getImageName(List<Parameter> parameters) {
        return findParameter(parameters, IMAGE_NAME);
    }

    public static Parameter newImageNameParameter(String imageName) {
        return new Parameter(IMAGE_NAME, imageName);
    }

    public static JobType getJobType(List<Parameter> parameters) {
        String value = findParameter(parameters, JOB_TYPE);
        if (value == null) {
            return null;
        }
        return JobType.valueOf(value);
    }

    public static Parameter newJobTypeParameter(JobType jobType) {
        return new Parameter(JOB_TYPE, jobType.name());
    }

    public static String getVersion(List<Parameter> parameters) {
        return findParameter(parameters, VERSION);
    }

    public static Parameter newVersionParameter(String version) {
        return new Parameter(VERSION, version);
    }

    public static String getIamProfile(List<Parameter> parameters) {
        return findParameter(parameters, IAM_PROFILE);
    }

    public static Parameter newIamProfileParameter(String iamProfile) {
        return new Parameter(IAM_PROFILE, iamProfile);
    }

    public static Map<String, String> getLabels(List<Parameter> parameters) {
        String jsonValue = findParameter(parameters, LABELS);
        if (jsonValue == null) {
            return Collections.emptyMap();
        }
        try {
            return MAPPER.readValue(jsonValue, MAP_TYPE_REF);
        } catch (IOException e) {
            throw new IllegalArgumentException("Labels state is not valid JSON object: " + jsonValue, e);
        }
    }

    public static Parameter newLabelsParameter(Map<String, String> labels) {
        try {
            return new Parameter(LABELS, MAPPER.writeValueAsString(labels));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize labels map to JSON", e);
        }
    }

    public static String getJobGroupStack(List<Parameter> parameters) {
        return findParameter(parameters, JOB_GROUP_STACK);
    }

    public static Parameter newJobGroupStack(String jobGroupStack) {
        return new Parameter(JOB_GROUP_STACK, jobGroupStack);
    }

    public static String getJobGroupDetail(List<Parameter> parameters) {
        return findParameter(parameters, JOB_GROUP_DETAIL);
    }

    public static Parameter newJobGroupDetail(String jobGroupDetail) {
        return new Parameter(JOB_GROUP_DETAIL, jobGroupDetail);
    }

    public static String getJobGroupSeq(List<Parameter> parameters) {
        return findParameter(parameters, JOB_GROUP_SEQ);
    }

    public static String getCapacityGroup(List<Parameter> parameters) {
        return findParameter(parameters, JOB_CAPACITY_GROUP);
    }

    public static MigrationPolicy getMigrationPolicy(List<Parameter> parameters) {
        String jsonValue = findParameter(parameters, JOB_MIGRATION_POLICY);
        if (jsonValue == null) {
            return null;
        }
        try {
            return MAPPER.readValue(jsonValue, MigrationPolicy.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Cannot convert json value to migrationPolicy: " + e.getMessage(), e);
        }
    }

    public static Parameter newJobGroupSequence(String jobGroupSequence) {
        return new Parameter(JOB_GROUP_SEQ, jobGroupSequence);
    }

    public static Parameter newCapacityGroup(String capacityGroup) {
        return new Parameter(JOB_CAPACITY_GROUP, capacityGroup);
    }

    public static Parameter newMigrationPolicy(MigrationPolicy migrationPolicy) {
        try {
            return new Parameter(JOB_MIGRATION_POLICY, MAPPER.writeValueAsString(migrationPolicy));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize migrationPolicy map to JSON", e);
        }
    }

    public static String getJobIdSequence(List<Parameter> parameters) {
        return findParameter(parameters, JOB_ID_SEQUENCE);
    }

    public static Parameter newJobIdSequence(String jobIdSequence) {
        return new Parameter(JOB_ID_SEQUENCE, jobIdSequence);
    }

    public static int[] getPortsArray(List<Parameter> parameters) {
        List<Integer> ports = getPorts(parameters);
        int[] array = new int[ports.size()];
        for (int i = 0; i < array.length; i++) {
            array[i] = ports.get(i);
        }
        return array;
    }

    public static List<Integer> getPorts(List<Parameter> parameters) {
        String value = findParameter(parameters, REQUESTED_PORTS);
        if (value == null || (value = value.trim()).isEmpty()) {
            return Collections.emptyList();
        }
        String[] portValues = COMA_SPLIT_RE.split(value);
        List<Integer> ports = new ArrayList<>(portValues.length);
        try {
            for (String pv : portValues) {
                ports.add(Integer.parseInt(pv.trim()));
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Ports list contains non-integer values: " + value, e);
        }
        return ports;
    }

    public static Parameter newPortsParameter(int[] ports) {
        StringBuilder sb = new StringBuilder();
        if (ports.length > 0) {
            sb.append(ports[0]);
            for (int i = 1; i < ports.length; i++) {
                sb.append(',').append(ports[i]);
            }
        }
        return new Parameter(REQUESTED_PORTS, sb.toString());
    }

    public static String getEntryPoint(List<Parameter> parameters) {
        return findParameter(parameters, ENTRY_POINT);
    }

    public static Parameter newEntryPointParameter(String entryPoint) {
        return new Parameter(ENTRY_POINT, entryPoint);
    }

    public static boolean getInService(List<Parameter> parameters) {
        return "true".equals(findParameter(parameters, IN_SERVICE));
    }

    public static Parameter newInServiceParameter(boolean inService) {
        return new Parameter(IN_SERVICE, Boolean.toString(inService));
    }

    public static List<Parameter> updateInService(List<Parameter> parameters, boolean inService) {
        return updateParameter(parameters, IN_SERVICE, Boolean.toString(inService));
    }

    public static List<String> getSecurityGroups(List<Parameter> parameters) {
        String parameter = findParameter(parameters, SECURITY_GROUPS);
        if (parameter == null || (parameter = parameter.trim()).isEmpty()) {
            return Collections.emptyList();
        }
        return asList(COMA_SPLIT_RE.split(parameter));
    }

    public static Parameter newSecurityGroupsParameter(List<String> securityGroups) {
        StringBuilder sb = new StringBuilder();
        if (!securityGroups.isEmpty()) {
            Iterator<String> it = securityGroups.iterator();
            sb.append(it.next());
            while (it.hasNext()) {
                sb.append(',').append(it.next());
            }

        }
        return new Parameter(SECURITY_GROUPS, sb.toString());
    }

    public static EfsMount getEfs(List<Parameter> parameters) {
        String jsonValue = findParameter(parameters, EFS);
        if (jsonValue == null) {
            return null;
        }
        try {
            return MAPPER.readValue(jsonValue, EfsMount.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("EFS state is not valid JSON object: " + jsonValue, e);
        }
    }

    public static Parameter newEfsParameter(EfsMount efsParams) {
        try {
            return new Parameter(EFS, MAPPER.writeValueAsString(efsParams));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize EFS map to JSON", e);
        }
    }

    public static List<EfsMount> getEfsMounts(List<Parameter> parameters) {
        String jsonValue = findParameter(parameters, EFS_MOUNTS);
        if (jsonValue == null) {
            return null;
        }
        try {
            return MAPPER.readValue(jsonValue, EFS_MOUNTS_TYPE_REF);
        } catch (IOException e) {
            throw new IllegalArgumentException("EFS state is not valid JSON object: " + jsonValue, e);
        }
    }

    public static Parameter newEfsMountsParameter(List<EfsMount> efsMounts) {
        try {
            return new Parameter(EFS_MOUNTS, MAPPER.writeValueAsString(efsMounts));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize EFS Mounts list to JSON", e);
        }
    }

    public static Map<String, String> getEnv(List<Parameter> parameters) {
        String jsonValue = findParameter(parameters, ENV);
        if (jsonValue == null) {
            return Collections.emptyMap();
        }
        try {
            return MAPPER.readValue(jsonValue, MAP_TYPE_REF);
        } catch (IOException e) {
            throw new IllegalArgumentException("Environment state is not valid JSON object: " + jsonValue, e);
        }
    }

    public static Parameter newEnvParameter(Map<String, String> env) {
        try {
            return new Parameter(ENV, MAPPER.writeValueAsString(env));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Cannot serialize environment map to JSON", e);
        }
    }

    public static boolean getRestartOnSuccess(List<Parameter> parameters) {
        String value = findParameter(parameters, RESTART_ON_SUCESS);
        return "true".equals(value);
    }

    public static Parameter newRestartOnSuccessParameter(boolean restartOnSuccess) {
        return new Parameter(RESTART_ON_SUCESS, Boolean.toString(restartOnSuccess));
    }

    public static String findParameter(List<Parameter> parameters, String parameter) {
        String result = null;
        for (Parameter p : parameters) {
            if (p.getName().equals(parameter)) {
                result = p.getValue();
                break;
            }
        }
        return result;
    }

    public static List<Parameter> updateParameter(List<Parameter> parameters, Parameter newParameter) {
        List<Parameter> newParameters = new ArrayList<>(parameters.size() + 1);

        for (Parameter p : parameters) {
            if (!p.getName().equals(newParameter.getName())) {
                newParameters.add(p);
            }
        }
        newParameters.add(newParameter);

        return newParameters;
    }

    public static List<Parameter> updateParameter(List<Parameter> parameters, String key, String value) {
        return updateParameter(parameters, new Parameter(key, value));
    }

    public static List<Parameter> removeParameter(List<Parameter> parameters, String key) {
        List<Parameter> newParameters = new ArrayList<>(parameters.size());
        for (Parameter p : parameters) {
            if (!p.getName().equals(key)) {
                newParameters.add(p);
            }
        }
        return newParameters;
    }

    /**
     * Merges left to right. If the same parameter name appears many times, the last value wins.
     * There is no particular order in the returned list guaranteed.
     */
    public static List<Parameter> mergeParameters(List<Parameter>... parameterLists) {
        HashMap<String, Parameter> name2Value = new HashMap<>();
        for (List<Parameter> l : parameterLists) {
            l.forEach(p -> name2Value.put(p.getName(), p));
        }
        return new ArrayList<>(name2Value.values());
    }
}
