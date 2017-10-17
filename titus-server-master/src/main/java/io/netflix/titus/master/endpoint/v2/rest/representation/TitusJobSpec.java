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

package io.netflix.titus.master.endpoint.v2.rest.representation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import io.netflix.titus.api.endpoint.v2.rest.representation.EfsMountRepresentation;
import io.netflix.titus.api.endpoint.v2.rest.representation.TitusJobType;
import io.netflix.titus.api.model.EfsMount;
import io.netflix.titus.api.model.MigrationPolicy;
import io.netflix.titus.api.model.v2.JobConstraints;
import io.netflix.titus.api.model.v2.MachineDefinition;
import io.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import io.netflix.titus.api.model.v2.descriptor.StageScalingPolicy;
import io.netflix.titus.api.model.v2.descriptor.StageSchedulingInfo;
import io.netflix.titus.api.model.v2.parameter.Parameter;
import io.netflix.titus.api.model.v2.parameter.Parameters;
import io.netflix.titus.api.model.v2.parameter.Parameters.JobType;
import io.netflix.titus.api.store.v2.V2JobMetadata;
import io.netflix.titus.api.store.v2.V2StageMetadata;
import io.netflix.titus.common.util.CollectionsExt;
import io.netflix.titus.common.util.StringExt;
import io.netflix.titus.master.config.MasterConfiguration;
import io.netflix.titus.master.config.MasterConfigurationConverters;
import io.netflix.titus.master.store.NamedJobs;

import static io.netflix.titus.common.util.CollectionsExt.ifNotEmpty;

public class TitusJobSpec {

    private static final ObjectMapper mapper;
    private static final double Default_Network_Bandwidth = 128.0;

    static {
        mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static TitusJobSpec getSpec(V2JobMetadata jobMetadata) {
        List<Parameter> parameters = jobMetadata.getParameters();
        JobType jobType = Parameters.getJobType(parameters);

        V2StageMetadata stage = jobMetadata.getStageMetadata(1);
        StageScalingPolicy scalingPolicy = stage.getScalingPolicy();

        Map<String, Double> scalars = stage.getMachineDefinition().getScalars();
        Double gpu = scalars == null ? null : scalars.get("gpu");

        return new TitusJobSpec(
                Parameters.getName(parameters),
                Parameters.getImageName(parameters),
                Parameters.getAppName(parameters),
                jobMetadata.getUser(),
                jobType == null ? null : (jobType == JobType.Batch ? TitusJobType.batch : TitusJobType.service),
                Parameters.getLabels(parameters),
                Parameters.getVersion(parameters),
                Parameters.getEntryPoint(parameters),
                Parameters.getInService(parameters),
                scalingPolicy == null ? 1 : scalingPolicy.getDesired(), // TODO We set here instances = desired. Is this ok?
                scalingPolicy == null ? 1 : scalingPolicy.getMin(), // TODO if not defined, assume 1 for everyhing. It this ok?
                scalingPolicy == null ? 1 : scalingPolicy.getMax(),
                scalingPolicy == null ? 1 : scalingPolicy.getDesired(),
                stage.getMachineDefinition().getCpuCores(),
                stage.getMachineDefinition().getMemoryMB(),
                stage.getMachineDefinition().getNetworkMbps(),
                stage.getMachineDefinition().getDiskMB(),
                Parameters.getPortsArray(parameters),
                gpu == null ? 0 : gpu.intValue(),
                Parameters.getEnv(parameters),
                jobMetadata.getSla().getRetries(),
                Parameters.getRestartOnSuccess(parameters),
                jobMetadata.getSla().getRuntimeLimitSecs(),
                stage.getAllocateIP(),
                Parameters.getIamProfile(parameters),
                Parameters.getSecurityGroups(parameters),
                toEfsMountRepresentation(Parameters.getEfs(parameters)),
                toEfsMountRepresentation(Parameters.getEfsMounts(parameters)),
                stage.getSoftConstraints(),
                stage.getHardConstraints(),
                Parameters.getJobGroupStack(parameters),
                Parameters.getJobGroupDetail(parameters),
                Parameters.getJobGroupSeq(parameters),
                Parameters.getCapacityGroup(parameters),
                Parameters.getMigrationPolicy(parameters)
        );
    }

    public static List<Parameter> getParameters(TitusJobSpec spec) {
        List<Parameter> parameters = new ArrayList<>();

        // TODO Remove after data migration is finished
        try {
            String jobSpecJson = mapper.writeValueAsString(spec);
            parameters.add(new Parameter("request", jobSpecJson));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot serialize TitusJobSpec into JSON");
        }

        parameters.add(Parameters.newCmdTypeParameter(NamedJobs.getTitusJobName().toLowerCase()));

        if (spec.getApplicationName() != null) {
            parameters.add(Parameters.newImageNameParameter(spec.getApplicationName()));
        }
        if (spec.getName() != null) {
            parameters.add(Parameters.newNameParameter(spec.getName()));
        }
        if (spec.getAppName() != null) {
            parameters.add(Parameters.newAppNameParameter(spec.getAppName()));
        }
        switch (spec.getType()) {
            case batch:
                parameters.add(Parameters.newJobTypeParameter(JobType.Batch));
                break;
            case service:
                parameters.add(Parameters.newJobTypeParameter(JobType.Service));
                break;
            default:
                throw new IllegalArgumentException("Unsupported job type " + spec.getType());
        }
        if (spec.getVersion() != null) {
            parameters.add(Parameters.newVersionParameter(spec.getVersion()));
        }
        if (spec.getIamProfile() != null) {
            parameters.add(Parameters.newIamProfileParameter(spec.getIamProfile()));
        }
        if (spec.getLabels() != null) {
            parameters.add(Parameters.newLabelsParameter(spec.getLabels()));
        }
        if (spec.getJobGroupStack() != null) {
            parameters.add(Parameters.newJobGroupStack(spec.getJobGroupStack()));
        }
        if (spec.getJobGroupDetail() != null) {
            parameters.add(Parameters.newJobGroupDetail(spec.getJobGroupDetail()));
        }
        if (spec.getJobGroupSequence() != null) {
            parameters.add(Parameters.newJobGroupSequence(spec.getJobGroupSequence()));
        }
        if (spec.getCapacityGroup() != null) {
            parameters.add(Parameters.newCapacityGroup(spec.getCapacityGroup()));
        }
        if (spec.getMigrationPolicy() != null) {
            parameters.add(Parameters.newMigrationPolicy(spec.getMigrationPolicy()));
        }
        if (spec.getJobIdSequence() != null) {
            parameters.add(Parameters.newJobIdSequence(spec.getJobIdSequence()));
        }
        if (spec.getPorts() != null) {
            parameters.add(Parameters.newPortsParameter(spec.getPorts()));
        }
        if (spec.getEntryPoint() != null) {
            parameters.add(Parameters.newEntryPointParameter(spec.getEntryPoint()));
        }
        if (spec.getSecurityGroups() != null) {
            parameters.add(Parameters.newSecurityGroupsParameter(spec.getSecurityGroups()));
        }
        if (spec.getEfs() != null) {
            parameters.add(Parameters.newEfsParameter(toEfsMount(spec.getEfs())));
        }
        if (spec.getEfsMounts() != null) {
            parameters.add(Parameters.newEfsMountsParameter(toEfsMounts(spec.getEfsMounts())));
        }
        parameters.add(Parameters.newInServiceParameter(spec.isInService()));
        if (spec.getEnv() != null) {
            parameters.add(Parameters.newEnvParameter(spec.getEnv()));
        }
        parameters.add(Parameters.newRestartOnSuccessParameter(spec.isRestartOnSuccess()));

        return parameters;
    }

    private static EfsMountRepresentation toEfsMountRepresentation(EfsMount efsMount) {
        if (efsMount == null) {
            return null;
        }
        return EfsMountRepresentation.newBuilder()
                .withEfsId(efsMount.getEfsId())
                .withEfsRelativeMountPoint(efsMount.getEfsRelativeMountPoint())
                .withMountPerm(EfsMountRepresentation.MountPerm.valueOf(efsMount.getMountPerm().name()))
                .withMountPoint(efsMount.getMountPoint())
                .build();
    }

    private static List<EfsMountRepresentation> toEfsMountRepresentation(List<EfsMount> efsMounts) {
        if (efsMounts == null) {
            return null;
        }
        return efsMounts.stream().map(TitusJobSpec::toEfsMountRepresentation).collect(Collectors.toList());
    }

    private static List<EfsMount> toEfsMounts(List<EfsMountRepresentation> efsMounts) {
        if (efsMounts == null) {
            return null;
        }
        return efsMounts.stream().map(TitusJobSpec::toEfsMount).collect(Collectors.toList());
    }

    private static EfsMount toEfsMount(EfsMountRepresentation representation) {
        if (representation == null) {
            return null;
        }
        return EfsMount.newBuilder()
                .withEfsId(representation.getEfsId())
                .withEfsRelativeMountPoint(representation.getEfsRelativeMountPoint())
                .withMountPerm(EfsMount.MountPerm.valueOf(representation.getMountPerm().name()))
                .withMountPoint(representation.getMountPoint())
                .build();
    }

    /**
     * Cleanup {@link TitusJobSpec} passed in argument.
     *
     * @return cleaned up {@link TitusJobSpec}
     */
    public static TitusJobSpec sanitize(MasterConfiguration config, TitusJobSpec original) {
        Builder builder = new Builder(original);

        // 'appName' must be non-empty string or null
        String appName = original.getAppName();
        if (appName != null && appName.trim().isEmpty()) {
            builder.appName(null);
        }

        // Security group list cannot be null (set default if null found)
        List<String> securityGroups = original.getSecurityGroups();
        if (securityGroups != null && !securityGroups.isEmpty()) {
            securityGroups = StringExt.trim(securityGroups);
        }
        if (securityGroups == null || securityGroups.isEmpty()) {
            builder.securityGroups(MasterConfigurationConverters.getDefaultSecurityGroupList(config));
        } else {
            builder.securityGroups(securityGroups);
        }

        // Default EFS to RW mode if EFS is requested but perm is not specified
        EfsMountRepresentation efsMount = original.getEfs();
        if (shouldAddEfsMountPerm(efsMount)) {
            builder.efs(copyWithEfsMount(efsMount));
        }
        ifNotEmpty(original.getEfsMounts(), () -> builder.efsMounts(reorderByEfsPathInclusion(addEfsDefaults(original))));

        if ((original.getRuntimeLimitSecs() == null || original.getRuntimeLimitSecs().equals(0L)) && original.getType() == TitusJobType.batch) {
            builder.runtimeLimitSecs(config.getDefaultRuntimeLimit());
        }

        // Convert 'null' values in env map to empty string
        if (!CollectionsExt.isNullOrEmpty(original.getEnv())) {
            Map<String, String> cleaned = new HashMap<>();
            original.getEnv().forEach((k, v) -> {
                if (k != null) {
                    cleaned.put(k, v == null ? "" : v);
                }
            });
            builder.env(cleaned);
        }

        // Convert 'null' values in labels to empty string
        if (!CollectionsExt.isNullOrEmpty(original.getLabels())) {
            Map<String, String> cleaned = new HashMap<>();
            original.getLabels().forEach((k, v) -> {
                if (k != null) {
                    cleaned.put(k, v == null ? "" : v);
                }
            });
            builder.labels(cleaned);
        }

        return builder.build();
    }

    private static List<EfsMountRepresentation> addEfsDefaults(TitusJobSpec original) {
        return original.getEfsMounts().stream()
                .map(em -> shouldAddEfsMountPerm(em) ? copyWithEfsMount(em) : em)
                .collect(Collectors.toList());
    }

    private static List<EfsMountRepresentation> reorderByEfsPathInclusion(List<EfsMountRepresentation> efsMounts) {
        efsMounts.sort(Comparator.comparing(EfsMountRepresentation::getMountPoint));
        return efsMounts;
    }

    private static EfsMountRepresentation copyWithEfsMount(EfsMountRepresentation efsMount) {
        return EfsMountRepresentation.newBuilder(efsMount).withMountPerm(EfsMountRepresentation.MountPerm.RW).build();
    }

    private static boolean shouldAddEfsMountPerm(EfsMountRepresentation efsMount) {
        return efsMount != null && efsMount.getEfsId() != null && efsMount.getMountPoint() != null && efsMount.getMountPerm() == null;
    }

    private final String name;
    private final String applicationName;
    private final String appName;
    private String user = null;
    private final TitusJobType type;
    private final Map<String, String> labels;
    private final String version;
    private final String entryPoint;
    private final boolean inService;
    private final int instances;
    private final int instancesMin;
    private final int instancesMax;
    private final int instancesDesired;
    private final double cpu;
    private final double memory;
    private final double networkMbps;
    private final double disk;
    private final int[] ports;
    private final int gpu;
    private final Map<String, String> env;
    private final int retries;
    private final boolean restartOnSuccess;
    private Long runtimeLimitSecs;
    private final boolean allocateIpAddress;
    private final String iamProfile;
    private final List<String> securityGroups;
    private final EfsMountRepresentation efs;
    private final List<EfsMountRepresentation> efsMounts;
    private final List<JobConstraints> softConstraints;
    private final List<JobConstraints> hardConstraints;
    private final SchedulingInfo schedulingInfo;
    private final String jobGroupStack;
    private final String jobGroupDetail;
    private final String jobGroupSequence;
    private final String capacityGroup;
    private final MigrationPolicy migrationPolicy;
    @JsonIgnore
    private final String jobIdSequence;

    private static String getJobIdSequence(String app, String stack, String detail, String seq) {
        return String.join("-", app, stack, detail, seq);
    }

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public TitusJobSpec(@JsonProperty("name") String name,
                        @JsonProperty("applicationName") String applicationName,
                        @JsonProperty("appName") String appName,
                        @JsonProperty("user") String user,
                        @JsonProperty("type") TitusJobType type,
                        @JsonProperty("labels") Map<String, String> labels,
                        @JsonProperty("version") String version,
                        @JsonProperty("entryPoint") String entryPoint,
                        @JsonProperty("inService") Boolean inService,
                        @JsonProperty("instances") int instances,
                        @JsonProperty("instancesMin") int instancesMin,
                        @JsonProperty("instancesMax") int instancesMax,
                        @JsonProperty("instancesDesired") int instancesDesired,
                        @JsonProperty("cpu") double cpu,
                        @JsonProperty("memory") double memory,
                        @JsonProperty("networkMbps") double networkMbps,
                        @JsonProperty("disk") double disk,
                        @JsonProperty("ports") int[] ports,
                        @JsonProperty("gpu") int gpu,
                        @JsonProperty("env") Map<String, String> env,
                        @JsonProperty("retries") int retries,
                        @JsonProperty("restartOnSuccess") boolean restartOnSuccess,
                        @JsonProperty("runtimeLimitSecs") Long runtimeLimitSecs,
                        @JsonProperty("allocateIpAddress") boolean allocateIpAddress,
                        @JsonProperty("iamProfile") String iamProfile,
                        @JsonProperty("securityGroups") List<String> securityGroups,
                        @JsonProperty("efs") EfsMountRepresentation efs,
                        @JsonProperty("efsMounts") List<EfsMountRepresentation> efsMounts,
                        @JsonProperty("softConstraints") List<JobConstraints> softConstraints,
                        @JsonProperty("hardConstraints") List<JobConstraints> hardConstraints,
                        @JsonProperty("jobGroupStack") String jobGroupStack,
                        @JsonProperty("jobGroupDetail") String jobGroupDetail,
                        @JsonProperty("jobGroupSequence") String jobGroupSequence,
                        @JsonProperty("capacityGroup") String capacityGroup,
                        @JsonProperty("migrationPolicy") MigrationPolicy migrationPolicy
    ) {
        this.name = name;
        this.applicationName = applicationName;
        this.appName = appName;
        this.user = user;
        this.type = type == null ? TitusJobType.batch : type;
        this.labels = labels;
        this.version = version;
        this.entryPoint = entryPoint;
        this.inService = inService == null ? true : inService;
        this.instances = this.type == TitusJobType.batch ? Math.max(1, instances) : instancesDesired; // default to 1 instances for batch
        this.instancesDesired = this.type == TitusJobType.batch ? this.instances : instancesDesired;
        this.instancesMax = this.type == TitusJobType.batch ? this.instances : instancesMax;
        this.instancesMin = this.type == TitusJobType.batch ? this.instances : instancesMin;
        this.cpu = cpu <= 0.0 ? 1.0 : cpu;
        this.memory = memory <= 0.0 ? 512.0 : memory;
        this.networkMbps = networkMbps <= 0.0 ? Default_Network_Bandwidth : networkMbps;
        this.disk = disk <= 0.0 ? 10_000.0 : disk;
        this.ports = ports;
        this.gpu = gpu;
        this.env = env;
        this.retries = this.type == TitusJobType.batch ? retries : Integer.MAX_VALUE;
        this.restartOnSuccess = restartOnSuccess;

        this.runtimeLimitSecs = runtimeLimitSecs;
        this.allocateIpAddress = allocateIpAddress;
        this.iamProfile = iamProfile;

        if (securityGroups == null) {
            this.securityGroups = null;
        } else {
            this.securityGroups = new ArrayList<>(securityGroups);
            this.securityGroups.sort(null); // null == use natural ordering
        }
        this.efs = efs;
        this.efsMounts = efsMounts;
        this.softConstraints = softConstraints;
        this.hardConstraints = hardConstraints;
        this.jobGroupStack = jobGroupStack;
        this.jobGroupDetail = jobGroupDetail;
        this.jobGroupSequence = jobGroupSequence;
        this.capacityGroup = capacityGroup == null || capacityGroup.trim().isEmpty() ? appName : capacityGroup;
        this.migrationPolicy = migrationPolicy;
        final Map<String, Double> scalars = new HashMap<>();
        if (this.gpu > 0) {
            scalars.put("gpu", (double) this.gpu);
        }
        StageScalingPolicy scalingPolicy = new StageScalingPolicy(1, instancesMin, instancesMax, instancesDesired, 0, 0, 0, null);
        schedulingInfo = new SchedulingInfo(Collections.singletonMap(1,
                new StageSchedulingInfo(this.instancesDesired,
                        new MachineDefinition(this.cpu, this.memory, this.networkMbps, this.disk, ports == null ? 0 : ports.length, scalars),
                        hardConstraints, softConstraints, this.securityGroups, allocateIpAddress, scalingPolicy, type == TitusJobType.service)
        ));
        this.jobIdSequence = jobGroupSequence == null || jobGroupSequence.isEmpty() ?
                null :
                getJobIdSequence(appName, jobGroupStack, jobGroupDetail, jobGroupSequence);
    }

    public static class Builder {
        private String name;
        private String applicationName;
        private String appName;
        private String user;
        private TitusJobType type = TitusJobType.batch;
        private Map<String, String> labels = new HashMap<>();
        private String version;
        private String entryPoint;
        private boolean inService;
        private int instances;
        private int instancesMin;
        private int instancesMax;
        private int instancesDesired;
        private double cpu;
        private double memory;
        private double networkMbps;
        private double disk;
        private int[] ports = new int[0];
        private int gpu = 0;
        private Map<String, String> env = new HashMap<>();
        private int retries;
        private boolean restartOnSuccess;
        private Long runtimeLimitSecs;
        private boolean allocateIpAddress;
        private String iamProfile;
        private List<String> securityGroups = new ArrayList<>();
        private EfsMountRepresentation efs;
        private List<EfsMountRepresentation> efsMounts = new ArrayList<>();
        private List<JobConstraints> softConstraints = new ArrayList<>();
        private List<JobConstraints> hardConstraints = new ArrayList<>();
        private String jobGroupStack;
        private String jobGroupDetail;
        private String jobGroupSequence;
        private String capacityGroup;
        private MigrationPolicy migrationPolicy;

        public Builder() {
        }

        public Builder(TitusJobSpec other) {
            this.name = other.name;
            this.applicationName = other.applicationName;
            this.appName = other.appName;
            this.user = other.user;
            this.type = other.type;
            this.labels = other.labels;
            this.version = other.version;
            this.entryPoint = other.entryPoint;
            this.inService = other.inService;
            this.instances = other.instances;
            this.instancesMin = other.instancesMin;
            this.instancesMax = other.instancesMax;
            this.instancesDesired = other.instancesDesired;
            this.cpu = other.cpu;
            this.memory = other.memory;
            this.networkMbps = other.networkMbps;
            this.disk = other.disk;
            this.ports = other.ports;
            this.gpu = other.gpu;
            this.env = other.env;
            this.retries = other.retries;
            this.restartOnSuccess = other.restartOnSuccess;
            this.runtimeLimitSecs = other.runtimeLimitSecs;
            this.allocateIpAddress = other.allocateIpAddress;
            this.iamProfile = other.iamProfile;
            this.securityGroups = other.securityGroups;
            this.efs = other.efs;
            this.efsMounts = other.efsMounts;
            this.softConstraints = other.softConstraints;
            this.hardConstraints = other.hardConstraints;
            this.jobGroupStack = other.jobGroupStack;
            this.jobGroupDetail = other.jobGroupDetail;
            this.jobGroupSequence = other.jobGroupSequence;
            this.capacityGroup = other.capacityGroup;
            this.migrationPolicy = other.migrationPolicy;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder appName(String appName) {
            this.appName = appName;
            return this;
        }

        public Builder user(String user) {
            this.user = user;
            return this;
        }

        public Builder type(TitusJobType type) {
            this.type = type;
            return this;
        }

        public Builder labels(Map<String, String> labels) {
            this.labels = labels;
            return this;
        }

        public Builder version(String version) {
            this.version = version;
            return this;
        }

        public Builder entryPoint(String ep) {
            this.entryPoint = ep;
            return this;
        }

        public Builder inService(boolean inService) {
            this.inService = inService;
            return this;
        }

        public Builder instances(int i) {
            this.instances = i;
            return this;
        }

        public Builder instancesMin(int i) {
            this.instancesMin = i;
            return this;
        }

        public Builder instancesMax(int i) {
            this.instancesMax = i;
            return this;
        }

        public Builder instancesDesired(int i) {
            this.instancesDesired = i;
            return this;
        }

        public Builder cpu(double cpu) {
            this.cpu = cpu;
            return this;
        }

        public Builder memory(double memory) {
            this.memory = memory;
            return this;
        }

        public Builder networkMbps(double networkMbps) {
            this.networkMbps = networkMbps;
            return this;
        }

        public Builder disk(double disk) {
            this.disk = disk;
            return this;
        }

        public Builder ports(int[] ports) {
            this.ports = ports;
            return this;
        }

        public Builder gpu(int gpu) {
            this.gpu = gpu;
            return this;
        }

        public Builder env(Map<String, String> env) {
            this.env = env;
            return this;
        }

        public Builder retries(int r) {
            this.retries = r;
            return this;
        }

        public Builder restartOnSuccess(boolean restartOnSuccess) {
            this.restartOnSuccess = restartOnSuccess;
            return this;
        }

        public Builder runtimeLimitSecs(Long runtimeLimitSecs) {
            this.runtimeLimitSecs = runtimeLimitSecs;
            return this;
        }

        public Builder allocateIpAddress(boolean allocateIpAddress) {
            this.allocateIpAddress = allocateIpAddress;
            return this;
        }

        public Builder iamProfile(String iamProfile) {
            this.iamProfile = iamProfile;
            return this;
        }

        public Builder securityGroups(List<String> securityGroups) {
            this.securityGroups = securityGroups;
            return this;
        }

        public Builder efs(EfsMountRepresentation efs) {
            this.efs = efs;
            return this;
        }

        public Builder efsMounts(List<EfsMountRepresentation> efsMounts) {
            this.efsMounts = efsMounts;
            return this;
        }

        public Builder softConstraints(List<JobConstraints> softConstraints) {
            this.softConstraints = softConstraints;
            return this;
        }

        public Builder hardConstraints(List<JobConstraints> hardConstraints) {
            this.hardConstraints = hardConstraints;
            return this;
        }

        public Builder jobGroupStack(String jobGroupStack) {
            this.jobGroupStack = jobGroupStack;
            return this;
        }

        public Builder jobGroupDetail(String jobGroupDetail) {
            this.jobGroupDetail = jobGroupDetail;
            return this;
        }

        public Builder jobGroupSequence(String jobGroupSequence) {
            this.jobGroupSequence = jobGroupSequence;
            return this;
        }

        public Builder capacityGroup(String capacityGroup) {
            this.capacityGroup = capacityGroup;
            return this;
        }

        public Builder migrationPolicy(MigrationPolicy migrationPolicy) {
            this.migrationPolicy = migrationPolicy;
            return this;
        }

        public TitusJobSpec build() {
            if (this.securityGroups == null) {
                throw new IllegalArgumentException("Security groups not defined");
            }
            return new TitusJobSpec(this.name, this.applicationName, this.appName, this.user, this.type, this.labels,
                    this.version, this.entryPoint, this.inService, this.instances, this.instancesMin, this.instancesMax, this.instancesDesired,
                    this.cpu, this.memory, this.networkMbps, this.disk, this.ports, this.gpu, this.env, this.retries, this.restartOnSuccess,
                    this.runtimeLimitSecs, this.allocateIpAddress, this.iamProfile, this.securityGroups, this.efs, this.efsMounts,
                    this.softConstraints, this.hardConstraints, this.jobGroupStack, this.jobGroupDetail, this.jobGroupSequence, this.capacityGroup,
                    this.migrationPolicy);
        }


    }

    public String getName() {
        return name;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getAppName() {
        return appName;
    }

    public String getUser() {
        return user;
    }

    public TitusJobType getType() {
        return type;
    }

    public Map<String, String> getLabels() {
        return labels;
    }

    public String getVersion() {
        return version;
    }

    public String getEntryPoint() {
        return entryPoint;
    }

    public boolean isInService() {
        return inService;
    }

    public int getInstances() {
        return instances;
    }

    public int getInstancesMin() {
        return instancesMin;
    }

    public int getInstancesMax() {
        return instancesMax;
    }

    public int getInstancesDesired() {
        return instancesDesired;
    }

    public double getCpu() {
        return cpu;
    }

    public double getMemory() {
        return memory;
    }

    public double getNetworkMbps() {
        return networkMbps;
    }

    public double getDisk() {
        return disk;
    }

    public int[] getPorts() {
        return ports;
    }

    public int getGpu() {
        return gpu;
    }

    public Map<String, String> getEnv() {
        return env;
    }

    public int getRetries() {
        return retries;
    }

    public boolean isRestartOnSuccess() {
        return restartOnSuccess;
    }

    public Long getRuntimeLimitSecs() {
        return runtimeLimitSecs;
    }

    public boolean isAllocateIpAddress() {
        return allocateIpAddress;
    }

    public String getIamProfile() {
        return iamProfile;
    }

    public List<String> getSecurityGroups() {
        return securityGroups;
    }

    public EfsMountRepresentation getEfs() {
        return efs;
    }

    public List<EfsMountRepresentation> getEfsMounts() {
        return efsMounts;
    }

    public List<JobConstraints> getSoftConstraints() {
        return softConstraints;
    }

    public List<JobConstraints> getHardConstraints() {
        return hardConstraints;
    }

    @JsonIgnore
    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    public String getJobGroupStack() {
        return jobGroupStack;
    }

    public String getJobGroupDetail() {
        return jobGroupDetail;
    }

    public String getJobGroupSequence() {
        return jobGroupSequence;
    }

    public String getCapacityGroup() {
        return capacityGroup;
    }

    public MigrationPolicy getMigrationPolicy() {
        return migrationPolicy;
    }

    @JsonIgnore
    public String getJobIdSequence() {
        return jobIdSequence;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass())
                .add("dockerImage", this.getApplicationName())
                .add("version", this.getVersion())
                .add("appName", this.getAppName())
                .add("cpu", this.getCpu())
                .add("memory", this.getMemory())
                .add("networkMbps", this.getNetworkMbps())
                .add("disk", this.getDisk())
                .add("instances", this.getInstances())
                .add("instancesMin", this.getInstancesMin())
                .add("instancesDesired", this.getInstancesDesired())
                .add("instancesMax", this.getInstancesMax())
                .add("entryPoint", this.getEntryPoint())
                .add("env", this.getEnv())
                .add("ports", this.getPorts())
                .add("iamProfile", this.getIamProfile())
                .add("securityGroups", this.getSecurityGroups())
                .add("jobGroupStack", this.getJobGroupStack())
                .add("jobGroupDetail", this.getJobGroupDetail())
                .add("jobGroupSequence", this.getJobGroupSequence())
                .add("labels", this.getLabels())
                .add("capacityGroup", this.getCapacityGroup())
                .add("migrationPolicy", this.getMigrationPolicy())
                .toString();
    }
}