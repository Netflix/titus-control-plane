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

package com.netflix.titus.master.store;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.titus.api.audit.model.AuditLogEvent;
import com.netflix.titus.api.audit.service.AuditLogService;
import com.netflix.titus.api.model.v2.JobOwner;
import com.netflix.titus.api.model.v2.JobSla;
import com.netflix.titus.api.model.v2.NamedJobDefinition;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.job.V2JobOperations;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.functions.Action2;
import rx.functions.Func1;
import rx.schedulers.Schedulers;
import rx.subjects.ReplaySubject;

public class NamedJobs {

    public static final String TitusJobName = "Titus";
    public static final String CommandJobName = "Command";

    public static String getTitusJobName() {
        return TitusJobName;
    }

    public static String getCommandJobName() {
        return CommandJobName;
    }

    public static boolean isStreamJobName(String name) {
        return !CommandJobName.equals(name) && !TitusJobName.equals(name);
    }

    static class NamedJobLock {
        private final ConcurrentMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();

        synchronized AutoCloseable obtainLock(final String jobName) {
            ReentrantLock newLock = new ReentrantLock();
            final ReentrantLock oldLock = locks.putIfAbsent(jobName, newLock);
            final ReentrantLock lock = oldLock == null ? newLock : oldLock;
            lock.lock();
            return new AutoCloseable() {
                @Override
                public void close() throws Exception {
                    lock.unlock();
                    if (!lock.isLocked()) {
                        locks.remove(jobName);
                    }
                }
            };
        }
    }

    public static class JobIdForSubmit {
        private final String jobId;
        private final boolean isNewJobId;

        public JobIdForSubmit(String jobId, boolean newJobId) {
            this.jobId = jobId;
            isNewJobId = newJobId;
        }

        public String getJobId() {
            return jobId;
        }

        public boolean isNewJobId() {
            return isNewJobId;
        }
    }

    private final ObjectMapper mapper = new ObjectMapper();

    private final MasterConfiguration config;
    private final ConcurrentMap<String, NamedJob> namedJobsMap;
    private static final Logger logger = LoggerFactory.getLogger(NamedJobs.class);
    private V2StorageProvider storageProvider;
    private final ReplaySubject<NamedJob> namedJobsSubject;
    private final ConcurrentMap<String, V2JobMgrIntf> jobMgrConcurrentMap;
    private final V2JobOperations jobOps;
    private final TriggerOperator triggerOperator;
    private final NamedJobLock namedJobLock = new NamedJobLock();
    private final AuditLogService auditLogService;

    public NamedJobs(MasterConfiguration config,
                     ConcurrentMap<String, NamedJob> namedJobsMap,
                     ReplaySubject<NamedJob> namedJobBehaviorSubject,
                     ConcurrentMap<String, V2JobMgrIntf> jobMgrConcurrentMap,
                     V2JobOperations jobOps,
                     TriggerOperator triggerOperator,
                     AuditLogService auditLogService) {
        this.config = config;
        this.namedJobsMap = namedJobsMap;
        this.namedJobsSubject = namedJobBehaviorSubject;
        this.jobMgrConcurrentMap = jobMgrConcurrentMap;
        this.jobOps = jobOps;
        this.triggerOperator = triggerOperator;
        this.auditLogService = auditLogService;
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public void init(V2StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
        boolean foundCommandJob = false;
        boolean foundTitusJob = false;
        try {
            for (NamedJob job : storageProvider.initNamedJobs()) {
                job.setConfig(config);
                job.setJobOps(jobOps);
                job.setTriggerOperator(triggerOperator);
                if (this.namedJobsMap.putIfAbsent(job.getName(), job) != null) {
                    throw new IllegalStateException("Unexpected to add duplicate namedJob entry for " + job.getName());
                }
                job.setStorageProvider(storageProvider);
                this.namedJobsSubject.onNext(job);
                if (CommandJobName.equals(job.getName())) {
                    foundCommandJob = true;
                }
                if (TitusJobName.equals(job.getName())) {
                    foundTitusJob = true;
                }
            }
            if (!foundCommandJob) {
                NamedJob job = new NamedJob(config, jobOps, triggerOperator, CommandJobName, null, new NamedJob.SLA(triggerOperator, 0, 0, null, null), null, null, 0L, false);
                storageProvider.storeNewNamedJob(job);
                namedJobsMap.put(job.getName(), job);
            }
            if (!foundTitusJob) {
                NamedJob job = new NamedJob(config, jobOps, triggerOperator, TitusJobName, null, new NamedJob.SLA(triggerOperator, 0, 0, null, null), null, null, 0L, false);
                storageProvider.storeNewNamedJob(job);
                namedJobsMap.put(job.getName(), job);
            }
            if (config.getStoreCompletedJobsForNamedJob()) {
                final AtomicReference<Throwable> errorRef = new AtomicReference<>();
                storageProvider.initNamedJobCompletedJobs()
                        .doOnNext(cj -> {
                            final NamedJob namedJob = namedJobsMap.get(cj.getName());
                            if (namedJob != null) {
                                namedJob.initCompletedJob(cj);
                            }
                        })
                        .doOnError(errorRef::set)
                        .toBlocking()
                        .lastOrDefault(null);
                if (errorRef.get() != null) {
                    throw new IOException(errorRef.get());
                }
            }
        } catch (IOException | JobNameAlreadyExistsException e) {
            // can't handle this
            throw new IllegalStateException(e.getMessage(), e);
        }
        Schedulers.computation().createWorker().schedulePeriodically(() -> {
            for (NamedJob job : namedJobsMap.values()) {
                job.enforceSla();
            }
        }, 60, 60, TimeUnit.SECONDS);
    }

    public void shutdown() {
    }

    public NamedJob getJobByName(String name) {
        return namedJobsMap.get(name);
    }

    public Collection<NamedJob> getAllNamedJobs() {
        return namedJobsMap.isEmpty() ? Collections.emptyList() : Collections.unmodifiableCollection(namedJobsMap.values());
    }

    public void deleteJob(String jobId) throws IOException {
        final NamedJob namedJob = namedJobsMap.get(NamedJob.getJobName(jobId));
        if (namedJob != null) {
            final V2JobMgrIntf jobMgr = jobMgrConcurrentMap.get(jobId);
            namedJob.removeJobMgr(jobMgr, jobId);
            storageProvider.removeCompledtedJobForNamedJob(namedJob.getName(), jobId);
        }
    }

    public NamedJob createNamedJob(NamedJobDefinition namedJobDefinition) throws InvalidNamedJobException {
        final String name = namedJobDefinition.getJobDefinition().getName();
        final JobOwner owner = namedJobDefinition.getOwner();
        if (!NamedJob.isValidJobName(name)) {
            throw new InvalidNamedJobException("Invalid name for a job: " + name);
        }
        if (namedJobsMap.putIfAbsent(name, new NamedJob(config, jobOps, triggerOperator, name, null, null, namedJobDefinition.getJobDefinition().getParameters(),
                owner, 0, false)) != null) {
            throw new InvalidNamedJobException(name + " named job already exists");
        }
        NamedJob job = namedJobsMap.get(name);
        job.setStorageProvider(storageProvider);
        boolean success = false;
        try (AutoCloseable lock = job.obtainLock()) {
            job.addJar(new NamedJob.Jar(namedJobDefinition.getJobDefinition().getJobJarFileLocation(),
                    System.currentTimeMillis(), namedJobDefinition.getJobDefinition().getVersion(),
                    namedJobDefinition.getJobDefinition().getSchedulingInfo()));
            job.setSla(new NamedJob.SLA(triggerOperator, namedJobDefinition.getJobDefinition().getSlaMin(),
                    namedJobDefinition.getJobDefinition().getSlaMax(),
                    namedJobDefinition.getJobDefinition().getCronSpec(),
                    namedJobDefinition.getJobDefinition().getCronPolicy()));
            storageProvider.storeNewNamedJob(job);
            success = true;
            auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.NAMED_JOB_CREATE, name,
                    "user=" + namedJobDefinition.getJobDefinition().getUser(), System.currentTimeMillis()));
        } catch (IOException e) {
            throw new InvalidNamedJobException(e.getMessage(), e);
        } catch (Exception e) {
            logLockError(e);
            throw new InvalidNamedJobException(name + ": " + e.getMessage(), e);
        } finally {
            if (!success) {
                namedJobsMap.remove(name);
            }
        }
        namedJobsSubject.onNext(job);
        return job;
    }

    public Action2<String, Collection<V2JobMgrIntf>> getNamedJobIdInitializer() {
        return (name, jobMgrs) -> {
            NamedJob job = namedJobsMap.get(name);
            if (job == null) {
                logger.error("Can't find NamedJob for name=" + name);
            } else {
                job.init(jobMgrs);
            }
        };
    }

    public void deleteNamedJob(String name, String user, Func1<String, Boolean> jobIdDeleter) throws NamedJobDeleteException {
        NamedJob job = namedJobsMap.get(name);
        if (job == null) {
            return;
        }
        try (AutoCloseable lock = job.obtainLock()) {
            if (!job.getIsActive()) {
                return;
            }
            String activeJobId = deleteAllJobs(name, jobIdDeleter);
            if (activeJobId != null) {
                logger.warn("Active job " + activeJobId + " exists, not deleting named job " + name);
                throw new NamedJobDeleteException("Active job exists - " + activeJobId);
            }
            boolean deleted = storageProvider.deleteNamedJob(name);
            job.setInactive();
            if (deleted) {
                namedJobsMap.remove(name);
                auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.NAMED_JOB_DELETE, name, "user=" + user, System.currentTimeMillis()));
            }
        } catch (NamedJobDeleteException njde) {
            throw njde;
        } catch (Exception e) {
            logger.error("Error deleting named job " + name + ": " + e.getMessage(), e);
            throw new NamedJobDeleteException("Unknown error deleting named job " + name, e);
        }
    }

    public void setDisabled(String name, String user, boolean status) throws InvalidNamedJobException {
        final NamedJob job = namedJobsMap.get(name);
        if (job == null) {
            return;
        }
        try (AutoCloseable lock = job.obtainLock()) {
            if (!job.getIsActive()) {
                throw new InvalidNamedJobException("Named job " + name + " not active");
            }
            if (job.getDisabled() == status) {
                return; // no-op
            }
            job.setDisabled(status);
            storageProvider.updateNamedJob(job);
            auditLogService.submit(new AuditLogEvent(
                    (status ? AuditLogEvent.Type.NAMED_JOB_DISABLED : AuditLogEvent.Type.NAMED_JOB_ENABLED),
                    name, "user=" + user, System.currentTimeMillis()));
        } catch (InvalidNamedJobException e) {
            throw e;
        } catch (IOException e) {
            throw new InvalidNamedJobException(e.getMessage(), e);
        } catch (Exception e) {
            logger.warn("Unexpected error locking job " + name + ": " + e.getMessage(), e);
            throw new InvalidNamedJobException("Internal error disabling job " + name, e);
        }
    }

    private String deleteAllJobs(String name, Func1<String, Boolean> jobIdDeleter) {
        // a simple linear search of all entries should suffice, this isn't expected to be called frequently
        boolean foundActive = false;
        List<V2JobMgrIntf> jobMgrs = new ArrayList<>();
        for (Map.Entry<String, V2JobMgrIntf> entry : jobMgrConcurrentMap.entrySet()) {
            V2JobMetadata jobMetadata = entry.getValue().getJobMetadata();
            if (jobMetadata.getName().equals(name)) {
                jobMgrs.add(entry.getValue());
                if (entry.getValue().isActive()) {
                    return jobMetadata.getJobId();
                }
            }
        }
        for (V2JobMgrIntf jobMgr : jobMgrs) {
            if (!jobIdDeleter.call(jobMgr.getJobId())) {
                return jobMgr.getJobId();
            }
        }
        return null;
    }

    public NamedJob updateNamedJob(NamedJobDefinition namedJobDefinition, boolean createIfNeeded) throws InvalidNamedJobException {
        final String name = namedJobDefinition.getJobDefinition().getName();
        NamedJob job = getJobByName(name);
        final int slaMin = namedJobDefinition.getJobDefinition().getSlaMin();
        final int slaMax = namedJobDefinition.getJobDefinition().getSlaMax();
        final String cronSpec = namedJobDefinition.getJobDefinition().getCronSpec();
        final NamedJobDefinition.CronPolicy cronPolicy = namedJobDefinition.getJobDefinition().getCronPolicy();
        if (job == null && createIfNeeded) {
            try {
                job = createNamedJob(namedJobDefinition);
            } catch (InvalidNamedJobException e) {
                job = getJobByName(name);
            }
        }
        if (job == null) {
            throw new InvalidNamedJobException(name + " named job doesn't exist");
        }
        String version = namedJobDefinition.getJobDefinition().getVersion();
        if (version == null || version.isEmpty()) {
            version = "" + System.currentTimeMillis();
        }
        SchedulingInfo schedulingInfo = namedJobDefinition.getJobDefinition().getSchedulingInfo();
        if (schedulingInfo == null) {
            final List<NamedJob.Jar> jars = job.getJars();
            schedulingInfo = jars.get(jars.size() - 1).getSchedulingInfo();
        }
        JobOwner owner = namedJobDefinition.getOwner();
        if (owner == null) {
            owner = job.getOwner();
        }
        List<Parameter> parameters = namedJobDefinition.getJobDefinition().getParameters();
        if (parameters == null || parameters.isEmpty()) {
            parameters = job.getParameters();
        }
        final NamedJob.Jar jar = new NamedJob.Jar(namedJobDefinition.getJobDefinition().getJobJarFileLocation(), System.currentTimeMillis(),
                version, schedulingInfo);
        job.addJar(jar);
        try (AutoCloseable lock = job.obtainLock()) {
            if (!job.getIsActive()) {
                throw new InvalidNamedJobException("Named job " + name + " not active");
            }
            job.setSla(new NamedJob.SLA(triggerOperator, slaMin, slaMax,
                    namedJobDefinition.getJobDefinition().getCronSpec(),
                    cronPolicy));
            job.setOwner(owner);
            job.setParameters(parameters);
            storageProvider.updateNamedJob(job);
        } catch (InvalidNamedJobException inje) {
            throw inje;
        } catch (IOException e) {
            throw new InvalidNamedJobException(e.getMessage(), e);
        } catch (Exception e) {
            logLockError(e);
            throw new InvalidNamedJobException(name + ": " + e.getMessage(), e);
        }
        try {
            auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.NAMED_JOB_UPDATE, name,
                    "user: " + namedJobDefinition.getJobDefinition().getUser() + ", sla: min=" + slaMin + ", max=" +
                            slaMax + "; jar=" + mapper.writeValueAsString(jar), System.currentTimeMillis()));
        } catch (JsonProcessingException e) {
            logger.warn("Error writing jar object value as json: " + e.getMessage());
        }
        return job;
    }

    public NamedJob quickUpdateNamedJob(String user, String name, URL jobJar, String version) throws InvalidNamedJobException {
        final NamedJob job = getJobByName(name);
        if (job == null) {
            throw new InvalidNamedJobException(name + " named job doesn't exist");
        }
        final NamedJob.Jar latestJar = job.getJar(null);
        if (version == null || version.isEmpty()) {
            version = "" + System.currentTimeMillis();
        }
        NamedJobDefinition namedJobDefinition = new NamedJobDefinition(
                new V2JobDefinition(
                        name, user, jobJar, version, job.getParameters(), null, 0,
                        latestJar.getSchedulingInfo(), job.getSla().getMin(), job.getSla().getMax(),
                        job.getSla().getCronSpec(), job.getSla().getCronPolicy()
                ),
                job.getOwner());
        return updateNamedJob(namedJobDefinition, false);
    }

    public void updateSla(String user, String name, NamedJob.SLA sla, boolean forceEnable) throws InvalidNamedJobException {
        if (sla == null) {
            throw new InvalidNamedJobException("Invalid null SLA");
        }
        final NamedJob job = getJobByName(name);
        if (job == null) {
            throw new InvalidNamedJobException(name + " named job doesn't exist");
        }
        if (forceEnable) {
            job.setDisabled(false);
        }
        job.setSla(sla);
        try {
            storageProvider.updateNamedJob(job);
        } catch (IOException e) {
            throw new InvalidNamedJobException(e.getMessage(), e);
        }
        auditLogService.submit(new AuditLogEvent(AuditLogEvent.Type.NAMED_JOB_UPDATE, name,
                "user: " + user + ", sla: min=" + sla.getMin() + ", max=" +
                        sla.getMax() + "; cronSpec=" + sla.getCronSpec() + ", cronPolicy=" + sla.getCronPolicy(), System.currentTimeMillis()));
    }

    public String quickSubmit(String jobName, String user) throws InvalidNamedJobException, InvalidJobException {
        final NamedJob job = getJobByName(jobName);
        if (job == null) {
            throw new InvalidNamedJobException(jobName + " named job doesn't exist");
        }
        return job.quickSubmit(user);
    }

    // Resolve fields of the job definition:
    //   - if job parameters not specified, inherit from existing ones in name job
    //   - if new jar given, expect scheduling info as well, and use them, else inherit from existing name job
    // Return new object containing the above resolved fields.
    public V2JobDefinition getResolvedJobDefinition(V2JobDefinition jobDefinition) throws InvalidNamedJobException {
        String version = jobDefinition.getVersion();
        NamedJob namedJob = getJobByName(jobDefinition.getName());
        if (namedJob == null) {
            throw new InvalidNamedJobException(jobDefinition.getName() + " named job doesn't exist");
        }
        List<Parameter> parameters = jobDefinition.getParameters();
        if (parameters == null || parameters.isEmpty()) {
            parameters = namedJob.getParameters();
        }
        NamedJob.Jar jar = null;
        SchedulingInfo schedulingInfo = null;
        if (jobDefinition.getJobJarFileLocation() != null) {
            if (jobDefinition.getSchedulingInfo() == null) {
                throw new InvalidNamedJobException("Scheduling info must be provided along with new job Jar");
            }
            schedulingInfo = jobDefinition.getSchedulingInfo();
            if (version == null || version.isEmpty()) {
                version = "" + System.currentTimeMillis();
            }
            jar = new NamedJob.Jar(jobDefinition.getJobJarFileLocation(), System.currentTimeMillis(), version,
                    jobDefinition.getSchedulingInfo());
            updateNamedJob(new NamedJobDefinition(
                            new V2JobDefinition(
                                    namedJob.getName(), null, jar.getUrl(), version, parameters, null, 0,
                                    schedulingInfo, namedJob.getSla().getMin(), namedJob.getSla().getMax(),
                                    namedJob.getSla().getCronSpec(), namedJob.getSla().getCronPolicy()
                            ),
                            namedJob.getOwner()
                    ),
                    false);
        }
        if (jar == null) {
            jar = namedJob.getJar(version);
            schedulingInfo = jobDefinition.getSchedulingInfo() == null ?
                    jar.getSchedulingInfo() :
                    getVerifiedSchedulingInfo(jar, jobDefinition.getSchedulingInfo(), version);
        }
        if (jar == null) {
            throw new InvalidNamedJobException(version + ": no such versioned jar found for named job " + jobDefinition.getName());
        }
        return new V2JobDefinition(
                jobDefinition.getName(), jobDefinition.getUser(), jar.getUrl(), jar.getVersion(),
                parameters, jobDefinition.getJobSla(), jobDefinition.getSubscriptionTimeoutSecs(), schedulingInfo,
                namedJob.getSla().getMin(), namedJob.getSla().getMax(),
                namedJob.getSla().getCronSpec(), namedJob.getSla().getCronPolicy()
        );
    }

    private SchedulingInfo getVerifiedSchedulingInfo(NamedJob.Jar jar, SchedulingInfo schedulingInfo, String version) throws InvalidNamedJobException {
        if (schedulingInfo.getStages().size() != jar.getSchedulingInfo().getStages().size()) {
            throw new InvalidNamedJobException("Mismatched scheduling info: expecting #stages=" +
                    jar.getSchedulingInfo().getStages().size() + " for given jar version [" + version +
                    "], where as, given scheduling info has #stages=" + schedulingInfo.getStages().size());
        }
        return schedulingInfo;
    }

    public JobIdForSubmit getJobIdForSubmit(String name, V2JobDefinition jobDefinition) throws InvalidNamedJobException {
        NamedJob job = getJobByName(name);
        if (job == null) {
            throw new InvalidNamedJobException(name + " named job doesn't exist");
        }
        try (AutoCloseable lock = job.obtainLock()) {
            if (job.getDisabled()) {
                throw new InvalidNamedJobException("Job " + name + " is disabled, submit disallowed");
            }
            String jobId = NamedJob.getJobId(name, job.getNextJobNumber());
            storageProvider.updateNamedJob(job);
            return new JobIdForSubmit(jobId, true);
        } catch (IOException e) {
            throw new InvalidNamedJobException(e.getMessage(), e);
        } catch (InvalidNamedJobException e) {
            throw e;
        } catch (Exception e) {
            logLockError(e);
            throw new InvalidNamedJobException("Unexpected to not get next id for job named " + name);
        }
    }

    static String getUniqueTag(String userProvidedType) {
        if (userProvidedType == null || userProvidedType.isEmpty()) {
            return null;
        }
        try {
            JSONObject jsonObject = new JSONObject(userProvidedType);
            return jsonObject.optString(JobSla.uniqueTagName);
        } catch (JSONException e) {
            return null;
        }
    }

    public String getLatestJobId(String name) throws InvalidNamedJobException {
        NamedJob job = getJobByName(name);
        if (job == null) {
            throw new InvalidNamedJobException(name + " named job doesn't exist");
        }
        if (!job.getIsActive()) {
            throw new InvalidNamedJobException(name + " named job not active");
        }
        return NamedJob.getJobId(name, job.getLastJobCount());
    }

    private void logLockError(Exception e) {
        logger.warn("Unexpected to not get a lock: " + e.getMessage());
    }

    public AutoCloseable lockJobName(String jobName) {
        return namedJobLock.obtainLock(jobName);
    }
}
