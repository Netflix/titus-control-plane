package pod

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/docker/distribution/reference"
	"github.com/hashicorp/go-multierror"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	AnnotationKeyInstanceType = "node.titus.netflix.com/itype"
	AnnotationKeyRegion       = "node.titus.netflix.com/region"
	AnnotationKeyStack        = "node.titus.netflix.com/stack"
	AnnotationKeyAZ           = "failure-domain.beta.kubernetes.io/zone"

	// Pod Networking
	AnnotationKeyEgressBandwidth  = "kubernetes.io/egress-bandwidth"
	AnnotationKeyIngressBandwidth = "kubernetes.io/ingress-bandwidth"

	// Pod ENI
	// AnnotationKeyIPAddress represents a generic "primary", could be ipv4 or v6
	AnnotationKeyIPAddress        = "network.netflix.com/address-ip"
	AnnotationKeyIPv4Address      = "network.netflix.com/address-ipv4"
	AnnotationKeyIPv4PrefixLength = "network.netflix.com/prefixlen-ipv4"
	AnnotationKeyIPv6Address      = "network.netflix.com/address-ipv6"
	AnnotationKeyIPv6PrefixLength = "network.netflix.com/prefixlen-ipv6"
	// AnnotationKeyIPv4TransitionAddress represents the "NAT" ip for certain IPv6-only modes
	AnnotationKeyIPv4TransitionAddress = "network.netflix.com/address-transition-ipv4"
	AnnotationKeyElasticIPv4Address    = "network.netflix.com/address-elastic-ipv4"
	AnnotationKeyElasticIPv6Address    = "network.netflix.com/address-elastic-ipv6"

	AnnotationKeyBranchEniID     = "network.netflix.com/branch-eni-id"
	AnnotationKeyBranchEniMac    = "network.netflix.com/branch-eni-mac"
	AnnotationKeyBranchEniVpcID  = "network.netflix.com/branch-eni-vpc"
	AnnotationKeyBranchEniSubnet = "network.netflix.com/branch-eni-subnet"

	AnnotationKeyTrunkEniID    = "network.netflix.com/trunk-eni-id"
	AnnotationKeyTrunkEniMac   = "network.netflix.com/trunk-eni-mac"
	AnnotationKeyTrunkEniVpcID = "network.netflix.com/trunk-eni-vpc"

	AnnotationKeyVlanID        = "network.netflix.com/vlan-id"
	AnnotationKeyAllocationIdx = "network.netflix.com/allocation-idx"

	// Security

	// matches kube2iam
	AnnotationKeyIAMRole              = "iam.amazonaws.com/role"
	AnnotationKeySecurityGroupsLegacy = "network.titus.netflix.com/securityGroups"
	// https://kubernetes.io/docs/tutorials/clusters/apparmor/#securing-a-pod
	AnnotationKeyPrefixAppArmor = "container.apparmor.security.beta.kubernetes.io"

	//
	// v1 pod spec annotations
	//

	// AnnotationKeyPodSchemaVersion is an integer specifying what schema version a pod was created with
	AnnotationKeyPodSchemaVersion = "pod.netflix.com/pod-schema-version"

	// Workload-specific fields

	AnnotationKeyWorkloadDetail     = "workload.netflix.com/detail"
	AnnotationKeyWorkloadName       = "workload.netflix.com/name"
	AnnotationKeyWorkloadOwnerEmail = "workload.netflix.com/owner-email"
	AnnotationKeyWorkloadSequence   = "workload.netflix.com/sequence"
	AnnotationKeyWorkloadStack      = "workload.netflix.com/stack"

	// Titus-specific fields

	AnnotationKeyJobAcceptedTimestampMs = "v3.job.titus.netflix.com/accepted-timestamp-ms"
	AnnotationKeyJobID                  = "v3.job.titus.netflix.com/id"
	AnnotationKeyJobType                = "v3.job.titus.netflix.com/type"
	AnnotationKeyJobDescriptor          = "v3.job.titus.netflix.com/descriptor"
	// AnnotationKeyPodTitusContainerInfo - to be removed once VK supports the full pod spec
	AnnotationKeyPodTitusContainerInfo = "pod.titus.netflix.com/container-info"
	// AnnotationKeyPodTitusEntrypointShellSplitting tells the executor to preserve the legacy shell splitting behaviour
	AnnotationKeyPodTitusEntrypointShellSplitting = "pod.titus.netflix.com/entrypoint-shell-splitting-enabled"
	// AnnotationKeyPodTitusSystemEnvVarNames tells the executor the names of the system-specified environment variables
	AnnotationKeyPodTitusSystemEnvVarNames = "pod.titus.netflix.com/system-env-var-names"
	// AnnotationKeyPodInjectedEnvVarNames tells the executor the names of the externally-injected environment variables,
	// which neither come from the user nor titus itself, and should be ignored for identify verification purposes
	AnnotationKeyPodInjectedEnvVarNames = "pod.titus.netflix.com/injected-env-var-names"
	// AnnotationKeyImageTagPrefix stores the original tag for the an image.
	// This is because on the v1 pod image field, there is only room for the digest and no room for the tag it came from
	AnnotationKeyImageTagPrefix = "pod.titus.netflix.com/image-tag-"

	// networking - used by the Titus CNI
	AnnotationKeySubnetsLegacy             = "network.titus.netflix.com/subnets"
	AnnotationKeyAccountIDLegacy           = "network.titus.netflix.com/accountId"
	AnnotationKeyNetworkAccountID          = "network.netflix.com/account-id"
	AnnotationKeyNetworkBurstingEnabled    = "network.netflix.com/network-bursting-enabled"
	AnnotationKeyNetworkAssignIPv6Address  = "network.netflix.com/assign-ipv6-address"
	AnnotationKeyNetworkElasticIPPool      = "network.netflix.com/elastic-ip-pool"
	AnnotationKeyNetworkElasticIPs         = "network.netflix.com/elastic-ips"
	AnnotationKeyNetworkIMDSRequireToken   = "network.netflix.com/imds-require-token"
	AnnotationKeyNetworkJumboFramesEnabled = "network.netflix.com/jumbo-frames-enabled"
	AnnotationKeyNetworkMode               = "network.netflix.com/network-mode"
	// AnnotationKeyEffectiveNetworkMode represents the network mode computed by the titus-executor
	// This may not be the same as the original (potentially unset) requested network mode
	AnnotationKeyEffectiveNetworkMode  = "network.netflix.com/effective-network-mode"
	AnnotationKeyNetworkSecurityGroups = "network.netflix.com/security-groups"
	AnnotationKeyNetworkSubnetIDs      = "network.netflix.com/subnet-ids"
	// TODO: deprecate this in favor of using the UUID annotation below
	AnnotationKeyNetworkStaticIPAllocationUUID = "network.netflix.com/static-ip-allocation-uuid"

	// storage
	AnnotationKeyStorageEBSVolumeID  = "ebs.volume.netflix.com/volume-id"
	AnnotationKeyStorageEBSMountPath = "ebs.volume.netflix.com/mount-path"
	AnnotationKeyStorageEBSMountPerm = "ebs.volume.netflix.com/mount-perm"
	AnnotationKeyStorageEBSFSType    = "ebs.volume.netflix.com/fs-type"

	// security

	AnnotationKeySecurityWorkloadMetadata    = "security.netflix.com/workload-metadata"
	AnnotationKeySecurityWorkloadMetadataSig = "security.netflix.com/workload-metadata-sig"

	// opportunistic resources (see control-plane and scheduler code)

	// AnnotationKeyOpportunisticCPU - assigned opportunistic CPUs
	AnnotationKeyOpportunisticCPU = "opportunistic.scheduler.titus.netflix.com/cpu"
	// AnnotationKeyOpportunisticResourceID - name of the opportunistic resource CRD used during scheduling
	AnnotationKeyOpportunisticResourceID = "opportunistic.scheduler.titus.netflix.com/id"

	// AnnotationKeyPredictionRuntime - predicted runtime (Go’s time.Duration format)
	AnnotationKeyPredictionRuntime = "predictions.scheduler.titus.netflix.com/runtime"
	// AnnotationKeyPredictionConfidence - confidence (percentile) of the prediction picked above
	AnnotationKeyPredictionConfidence = "predictions.scheduler.titus.netflix.com/confidence"
	// AnnotationKeyPredictionModelID - model uuid used for the runtime prediction picked above
	AnnotationKeyPredictionModelID = "predictions.scheduler.titus.netflix.com/model-id"
	// AnnotationKeyPredictionModelVersion - version of the model used for the prediction above
	AnnotationKeyPredictionModelVersion = "predictions.scheduler.titus.netflix.com/version"

	// AnnotationKeyPredictionABTestCell - cell allocation for prediction AB tests
	AnnotationKeyPredictionABTestCell = "predictions.scheduler.titus.netflix.com/ab-test"
	// AnnotationKeyPredictionPredictionAvailable - array of predictions available during job admission
	AnnotationKeyPredictionPredictionAvailable = "predictions.scheduler.titus.netflix.com/available"
	// AnnotationKeyPredictionSelectorInfo - metadata from the prediction selection algorithm
	AnnotationKeyPredictionSelectorInfo = "predictions.scheduler.titus.netflix.com/selector-info"

	// pod features

	AnnotationKeyPodCPUBurstingEnabled      = "pod.netflix.com/cpu-bursting-enabled"
	AnnotationKeyPodKvmEnabled              = "pod.netflix.com/kvm-enabled"
	AnnotationKeyPodFuseEnabled             = "pod.netflix.com/fuse-enabled"
	AnnotationKeyPodHostnameStyle           = "pod.netflix.com/hostname-style"
	AnnotationKeyPodOomScoreAdj             = "pod.netflix.com/oom-score-adj"
	AnnotationKeyPodSchedPolicy             = "pod.netflix.com/sched-policy"
	AnnotationKeyPodSeccompAgentNetEnabled  = "pod.netflix.com/seccomp-agent-net-enabled"
	AnnotationKeyPodSeccompAgentPerfEnabled = "pod.netflix.com/seccomp-agent-perf-enabled"
	AnnotationKeyPodTrafficSteeringEnabled  = "pod.netflix.com/traffic-steering-enabled"

	// container annotations (specified on a pod about a container)
	// Specific containers indicate they want to set something by appending
	// a prefix key with their container name ($name.containers.netflix.com).
	AnnotationKeySuffixContainers        = "containers.netflix.com"
	AnnotationKeySuffixContainersSidecar = "platform-sidecar"

	// logging config

	AnnotationKeyLogKeepLocalFile       = "log.netflix.com/keep-local-file-after-upload"
	AnnotationKeyLogS3BucketName        = "log.netflix.com/s3-bucket-name"
	AnnotationKeyLogS3PathPrefix        = "log.netflix.com/s3-path-prefix"
	AnnotationKeyLogS3WriterIAMRole     = "log.netflix.com/s3-writer-iam-role"
	AnnotationKeyLogStdioCheckInterval  = "log.netflix.com/stdio-check-interval"
	AnnotationKeyLogUploadThresholdTime = "log.netflix.com/upload-threshold-time"
	AnnotationKeyLogUploadCheckInterval = "log.netflix.com/upload-check-interval"
	AnnotationKeyLogUploadRegexp        = "log.netflix.com/upload-regexp"

	// service configuration

	AnnotationKeyServicePrefix = "service.netflix.com"

	// sidecar configuration

	AnnotationKeySuffixSidecars                      = "platform-sidecars.netflix.com"
	AnnotationKeySuffixSidecarsChannelOverride       = "channel-override"
	AnnotationKeySuffixSidecarsChannelOverrideReason = "channel-override-reason"
	// release = $channel/$version
	AnnotationKeySuffixSidecarsRelease = "release"

	// scheduling soft SLAs
	// priority handling in scheduling queue
	AnnotationKeySchedLatencyReq   = "scheduler.titus.netflix.com/sched-latency-req"
	AnnotationValSchedLatencyDelay = "delay"
	AnnotationValSchedLatencyFast  = "fast"
	// dynamic spreading behavior
	AnnotationKeySchedSpreadingReq    = "scheduler.titus.netflix.com/spreading-req"
	AnnotationValSchedSpreadingPack   = "pack"
	AnnotationValSchedSpreadingSpread = "spread"
)

func validateImage(image string) error {
	ref, err := reference.Parse(image)
	if err != nil {
		return err
	}

	_, digestOk := ref.(reference.Digested)
	_, taggedOk := ref.(reference.Tagged)

	if !digestOk && !taggedOk {
		return errors.New("image does not have a digest or tag")
	}

	return nil
}

func parseAnnotations(pod *corev1.Pod, pConf *Config) error {
	annotations := pod.GetAnnotations()
	userCtr := GetMainUserContainer(pod)
	if userCtr == nil {
		return errors.New("no containers found in pod")
	}

	boolAnnotations := []struct {
		key   string
		field **bool
	}{
		{
			key:   AnnotationKeyLogKeepLocalFile,
			field: &pConf.LogKeepLocalFile,
		},
		{
			key:   AnnotationKeyNetworkAssignIPv6Address,
			field: &pConf.AssignIPv6Address,
		},
		{
			key:   AnnotationKeyNetworkBurstingEnabled,
			field: &pConf.NetworkBurstingEnabled,
		},
		{
			key:   AnnotationKeyNetworkJumboFramesEnabled,
			field: &pConf.JumboFramesEnabled,
		},
		{
			key:   AnnotationKeyPodCPUBurstingEnabled,
			field: &pConf.CPUBurstingEnabled,
		},
		{
			key:   AnnotationKeyPodFuseEnabled,
			field: &pConf.FuseEnabled,
		},
		{
			key:   AnnotationKeyPodKvmEnabled,
			field: &pConf.KvmEnabled,
		},
		{
			key:   AnnotationKeyPodSeccompAgentNetEnabled,
			field: &pConf.SeccompAgentNetEnabled,
		},
		{
			key:   AnnotationKeyPodSeccompAgentPerfEnabled,
			field: &pConf.SeccompAgentPerfEnabled,
		},
		{
			key:   AnnotationKeyPodTrafficSteeringEnabled,
			field: &pConf.TrafficSteeringEnabled,
		},
		{
			key:   AnnotationKeyPodTitusEntrypointShellSplitting,
			field: &pConf.EntrypointShellSplitting,
		},
	}

	durationAnnotations := []struct {
		key   string
		field **time.Duration
	}{
		{
			key:   AnnotationKeyLogStdioCheckInterval,
			field: &pConf.LogStdioCheckInterval,
		},
		{
			key:   AnnotationKeyLogUploadCheckInterval,
			field: &pConf.LogUploadCheckInterval,
		},
		{
			key:   AnnotationKeyLogUploadThresholdTime,
			field: &pConf.LogUploadThresholdTime,
		},
	}

	resourceAnnotations := []struct {
		key   string
		field **resource.Quantity
	}{
		{
			key:   AnnotationKeyEgressBandwidth,
			field: &pConf.EgressBandwidth,
		},
		{
			key:   AnnotationKeyIngressBandwidth,
			field: &pConf.IngressBandwidth,
		},
	}

	stringAnnotations := []struct {
		key   string
		field **string
	}{
		{
			key:   AnnotationKeyPrefixAppArmor + "/" + userCtr.Name,
			field: &pConf.AppArmorProfile,
		},
		{
			key:   AnnotationKeyWorkloadDetail,
			field: &pConf.WorkloadDetail,
		},
		{
			key:   AnnotationKeyWorkloadName,
			field: &pConf.WorkloadName,
		},
		{
			key:   AnnotationKeyWorkloadOwnerEmail,
			field: &pConf.WorkloadOwnerEmail,
		},
		{
			key:   AnnotationKeyWorkloadSequence,
			field: &pConf.WorkloadSequence,
		},
		{
			key:   AnnotationKeyWorkloadStack,
			field: &pConf.WorkloadStack,
		},
		{
			key:   AnnotationKeyIAMRole,
			field: &pConf.IAMRole,
		},
		{
			key:   AnnotationKeyJobDescriptor,
			field: &pConf.JobDescriptor,
		},
		{
			key:   AnnotationKeyJobID,
			field: &pConf.JobID,
		},
		{
			key:   AnnotationKeyJobType,
			field: &pConf.JobType,
		},
		{
			key:   AnnotationKeyLogS3BucketName,
			field: &pConf.LogS3BucketName,
		},
		{
			key:   AnnotationKeyLogS3PathPrefix,
			field: &pConf.LogS3PathPrefix,
		},
		{
			key:   AnnotationKeyLogS3WriterIAMRole,
			field: &pConf.LogS3WriterIAMRole,
		},
		{
			key:   AnnotationKeyNetworkAccountID,
			field: &pConf.AccountID,
		},
		{
			key:   AnnotationKeyNetworkElasticIPPool,
			field: &pConf.ElasticIPPool,
		},
		{
			key:   AnnotationKeyNetworkElasticIPs,
			field: &pConf.ElasticIPs,
		},
		{
			key:   AnnotationKeyNetworkIMDSRequireToken,
			field: &pConf.IMDSRequireToken,
		},
		{
			key:   AnnotationKeyNetworkStaticIPAllocationUUID,
			field: &pConf.StaticIPAllocationUUID,
		},
		{
			key:   AnnotationKeyPodTitusContainerInfo,
			field: &pConf.ContainerInfo,
		},
		{
			key:   AnnotationKeyPodHostnameStyle,
			field: &pConf.HostnameStyle,
		},
		{
			key:   AnnotationKeyPodSchedPolicy,
			field: &pConf.SchedPolicy,
		},
		{
			key:   AnnotationKeySecurityWorkloadMetadata,
			field: &pConf.WorkloadMetadata,
		},
		{
			key:   AnnotationKeySecurityWorkloadMetadataSig,
			field: &pConf.WorkloadMetadataSig,
		},
		{
			key:   AnnotationKeyNetworkMode,
			field: &pConf.NetworkMode,
		},
	}

	uint32Annotations := []struct {
		key   string
		field **uint32
	}{
		{
			key:   AnnotationKeyPodSchemaVersion,
			field: &pConf.PodSchemaVersion,
		},
	}

	var err *multierror.Error

	for _, an := range stringAnnotations {
		val, ok := annotations[an.key]
		if ok {
			*an.field = &val
		}
	}

	if hostnameStyle, ok := annotations[AnnotationKeyPodHostnameStyle]; ok {
		if hostnameStyle != "ec2" && hostnameStyle != "" {
			err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid hostname style: %s", AnnotationKeyPodHostnameStyle, hostnameStyle))
		}
	}

	for _, an := range boolAnnotations {
		val, ok := annotations[an.key]
		if ok {
			boolVal, pErr := strconv.ParseBool(val)
			if pErr == nil {
				*an.field = &boolVal
			} else {
				err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid boolean value %s: %w", an.key, val, pErr))
			}
		}
	}

	for _, an := range uint32Annotations {
		val, ok := annotations[an.key]
		if ok {
			parsedVal, pErr := strconv.ParseUint(val, 10, 32)
			if pErr == nil {
				parsedUint32 := uint32(parsedVal)
				*an.field = &parsedUint32
			} else {
				err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid uint32 value %s: %w", an.key, val, pErr))
			}
		}
	}

	val, ok := annotations[AnnotationKeyJobAcceptedTimestampMs]
	if ok {
		parsedVal, pErr := strconv.ParseUint(val, 10, 64)
		if pErr == nil {
			parsedUint64 := uint64(parsedVal)
			pConf.JobAcceptedTimestampMs = &parsedUint64
		} else {
			err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid uint64 value %s: %w", AnnotationKeyJobAcceptedTimestampMs, val, pErr))
		}
	}

	val, ok = annotations[AnnotationKeyPodOomScoreAdj]
	if ok {
		parsedVal, pErr := strconv.ParseInt(val, 10, 32)
		if pErr == nil {
			parsedInt32 := int32(parsedVal)
			pConf.OomScoreAdj = &parsedInt32
		} else {
			err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid int32 value %s: %w", AnnotationKeyPodOomScoreAdj, val, pErr))
		}
	}

	for _, an := range resourceAnnotations {
		val, ok := annotations[an.key]
		if ok {
			resVal, pErr := resource.ParseQuantity(val)
			if pErr == nil {
				*an.field = &resVal
			} else {
				err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid resource value %s: %w", an.key, &resVal, pErr))
			}
		}
	}

	for _, an := range durationAnnotations {
		val, ok := annotations[an.key]
		if ok {
			durVal, pErr := time.ParseDuration(val)
			if pErr == nil {
				*an.field = &durVal
			} else {
				err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid duration value %s: %w", an.key, durVal, pErr))
			}
		}
	}

	if uploadRegexpVal, ok := annotations[AnnotationKeyLogUploadRegexp]; ok {
		uploadRegexp, pErr := regexp.Compile(uploadRegexpVal)
		if pErr == nil {
			pConf.LogUploadRegExp = uploadRegexp
		} else {
			err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid regexp value %s:  %w", uploadRegexpVal, AnnotationKeyLogUploadRegexp, pErr))
		}
	}

	if sgVal, ok := annotations[AnnotationKeyNetworkSecurityGroups]; ok {
		sgsSplit := strings.Split(strings.TrimSpace(sgVal), ",")
		sgIDs := []string{}
		for _, sg := range sgsSplit {
			sgIDs = append(sgIDs, strings.TrimSpace(sg))
		}
		pConf.SecurityGroupIDs = &sgIDs
	}

	if subVal, ok := annotations[AnnotationKeyNetworkSubnetIDs]; ok {
		subsSplit := strings.Split(strings.TrimSpace(subVal), ",")
		subIDs := []string{}
		for _, sub := range subsSplit {
			subIDs = append(subIDs, strings.TrimSpace(sub))
		}
		pConf.SubnetIDs = &subIDs
	}

	if envVal, ok := annotations[AnnotationKeyPodTitusSystemEnvVarNames]; ok {
		envsSplit := strings.Split(strings.TrimSpace(envVal), ",")
		for _, env := range envsSplit {
			pConf.SystemEnvVarNames = append(pConf.SystemEnvVarNames, strings.TrimSpace(env))
		}
	}

	if envVal, ok := annotations[AnnotationKeyPodInjectedEnvVarNames]; ok {
		envsSplit := strings.Split(strings.TrimSpace(envVal), ",")
		for _, env := range envsSplit {
			pConf.InjectedEnvVarNames = append(pConf.InjectedEnvVarNames, strings.TrimSpace(env))
		}
	}

	if pConf.SchedPolicy != nil && *pConf.SchedPolicy != "batch" && *pConf.SchedPolicy != "idle" {
		err = multierror.Append(err, fmt.Errorf("%s annotation is not a valid scheduler policy: %s", AnnotationKeyPodSchedPolicy, *pConf.SchedPolicy))
	}

	if sErr := parseServiceAnnotations(annotations, pConf); sErr != nil {
		err = multierror.Append(err, sErr)
	}

	if err == nil {
		return nil
	}
	return err.ErrorOrNil()
}

// Parse the "service.netflix.com/svc.v0.name" annotations
func parseServiceAnnotations(annotations map[string]string, pConf *Config) error {
	var err *multierror.Error
	sidecars := map[string]Sidecar{}

	for k, v := range annotations {
		if !strings.HasPrefix(k, AnnotationKeyServicePrefix) {
			continue
		}

		// name, version, value, eg: servicemesh.v2.image
		splitOut := strings.Split(strings.TrimPrefix(k, AnnotationKeyServicePrefix+"/"), ".")
		if len(splitOut) != 3 {
			err = multierror.Append(err, fmt.Errorf("annotation has an incorrect number of service configuration parameters: %s", k))
			continue
		}
		name := splitOut[0]
		version := splitOut[1]
		param := splitOut[2]

		sc, ok := sidecars[name+"."+version]
		if !ok {
			vInt, vErr := strconv.Atoi(strings.TrimPrefix(version, "v"))
			if vErr != nil {
				err = multierror.Append(err, fmt.Errorf("%s annotation has an incorrect service version number: %s", k, version))
				continue
			}

			sc = Sidecar{
				Name:    name,
				Version: vInt,
			}
		}

		if param == "enabled" {
			boolVal, pErr := strconv.ParseBool(v)
			if pErr != nil {
				err = multierror.Append(err, fmt.Errorf("%s annotation has an incorrect service enabled boolean value: %s", k, v))
				continue
			}
			sc.Enabled = boolVal
		}

		if param == "image" {
			if iErr := validateImage(v); iErr != nil {
				err = multierror.Append(err, fmt.Errorf("error parsing service image annotation: %s: %w", k, iErr))
				continue
			}
			sc.Image = v
		}

		sidecars[name+"."+version] = sc
	}

	for _, sc := range sidecars {
		pConf.Sidecars = append(pConf.Sidecars, sc)
	}

	if err == nil {
		return nil
	}
	return err.ErrorOrNil()
}

// PodSchemaVersion returns the pod schema version used to create a pod.
// If unset, returns 0
func PodSchemaVersion(pod *corev1.Pod) (uint32, error) {
	defaultVal := uint32(0)
	val, ok := pod.GetAnnotations()[AnnotationKeyPodSchemaVersion]
	if !ok {
		return defaultVal, nil
	}

	parsedVal, err := strconv.ParseUint(val, 10, 32)
	if err != nil {
		return defaultVal, fmt.Errorf("annotation is not a valid uint32 value: %s", AnnotationKeyPodSchemaVersion)
	}

	return uint32(parsedVal), nil
}

// ContainerAnnotation forms an annotation key referencing a particular container.
func ContainerAnnotation(containerName, suffix string) string {
	return fmt.Sprintf("%s.%s/%s", containerName, AnnotationKeySuffixContainers, suffix)
}

// IsPlatformSidecarContainer takes a container name and pod object,
// and can tell you if a particular container is a Platform Sidecar.
func IsPlatformSidecarContainer(name string, pod *corev1.Pod) bool {
	platformSidecarAnnotation := ContainerAnnotation(name, AnnotationKeySuffixContainersSidecar)
	_, ok := pod.Annotations[platformSidecarAnnotation]
	return ok
}

// SidecarAnnotation forms an annotation key referencing a particular sidecar.
func SidecarAnnotation(sidecarName, suffix string) string {
	return fmt.Sprintf("%s.%s/%s", sidecarName, AnnotationKeySuffixSidecars, suffix)
}

type PlatformSidecar struct {
	Name     string
	Channel  string
	ArgsJSON []byte
}

// PlatformSidecars parses sidecar-related annotations and returns a structured
// slice of platform sidecars.
func PlatformSidecars(annotations map[string]string) ([]PlatformSidecar, error) {
	var sidecars []PlatformSidecar
	for annotation, val := range annotations {
		if !strings.HasSuffix(annotation, "."+AnnotationKeySuffixSidecars) {
			continue
		}
		boolVal, err := strconv.ParseBool(val)
		if err != nil {
			return nil, fmt.Errorf("sidecar annotation %q must be a bool value: %v", annotation, err)
		}
		if !boolVal {
			continue
		}

		sidecar := PlatformSidecar{}
		sidecar.Name = strings.TrimSuffix(annotation, "."+AnnotationKeySuffixSidecars)
		channel, ok := annotations[SidecarAnnotation(sidecar.Name, "channel")]
		if !ok {
			return nil, fmt.Errorf("sidecar %q must have a channel specified via annotation %q", annotation, SidecarAnnotation(sidecar.Name, "channel"))
		}
		sidecar.Channel = channel
		if args, ok := annotations[SidecarAnnotation(sidecar.Name, "arguments")]; ok {
			sidecar.ArgsJSON = []byte(args)
		}
		sidecars = append(sidecars, sidecar)
	}

	return sidecars, nil
}
