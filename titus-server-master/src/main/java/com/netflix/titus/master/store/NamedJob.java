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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.netflix.fenzo.triggers.CronTrigger;
import com.netflix.fenzo.triggers.TriggerOperator;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.api.model.v2.JobOwner;
import com.netflix.titus.api.model.v2.NamedJobDefinition;
import com.netflix.titus.api.model.v2.V2JobDefinition;
import com.netflix.titus.api.model.v2.V2JobState;
import com.netflix.titus.api.model.v2.descriptor.SchedulingInfo;
import com.netflix.titus.api.model.v2.parameter.Parameter;
import com.netflix.titus.api.store.v2.InvalidJobException;
import com.netflix.titus.api.store.v2.V2JobMetadata;
import com.netflix.titus.master.config.MasterConfiguration;
import com.netflix.titus.master.job.V2JobMgrIntf;
import com.netflix.titus.master.job.V2JobOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.subjects.BehaviorSubject;

public class NamedJob {

    private static final Pattern VALID_JOB_ID_RE = Pattern.compile("^[A-Za-z]+[A-Za-z0-9+-_=:;]*");

    public static class CompletedJob {
        private final String name;
        private final String jobId;
        private final String version;
        private final V2JobState state;
        private final long submittedAt;
        private final long terminatedAt;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public CompletedJob(
                @JsonProperty("name") String name,
                @JsonProperty("jobId") String jobId,
                @JsonProperty("version") String version,
                @JsonProperty("state") V2JobState state,
                @JsonProperty("submittedAt") long submittedAt,
                @JsonProperty("terminatedAt") long terminatedAt
        ) {
            this.name = name;
            this.jobId = jobId;
            this.version = version;
            this.state = state;
            this.submittedAt = submittedAt;
            this.terminatedAt = terminatedAt;
        }

        public String getName() {
            return name;
        }

        public String getJobId() {
            return jobId;
        }

        public String getVersion() {
            return version;
        }

        public V2JobState getState() {
            return state;
        }

        public long getSubmittedAt() {
            return submittedAt;
        }

        public long getTerminatedAt() {
            return terminatedAt;
        }
    }

    public static class Jar {
        private final URL url;
        private final String version;
        private final long uploadedAt;
        private final SchedulingInfo schedulingInfo;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public Jar(@JsonProperty("url") URL url,
                   @JsonProperty("uploadedAt") long uploadedAt,
                   @JsonProperty("version") String version, @JsonProperty("schedulingInfo") SchedulingInfo schedulingInfo) {
            this.url = url;
            this.uploadedAt = uploadedAt;
            this.version = (version == null || version.isEmpty()) ?
                    "" + System.currentTimeMillis() :
                    version;
            this.schedulingInfo = schedulingInfo;
        }

        public URL getUrl() {
            return url;
        }

        public long getUploadedAt() {
            return uploadedAt;
        }

        public String getVersion() {
            return version;
        }

        public SchedulingInfo getSchedulingInfo() {
            return schedulingInfo;
        }
    }

    public static class SLA {

        private final int min;
        private final int max;
        private final String cronSpec;
        private final NamedJobDefinition.CronPolicy cronPolicy;
        @JsonIgnore
        private final boolean hasCronSpec;
        @JsonIgnore
        private final NamedJobDefinition.CronPolicy defaultPolicy = NamedJobDefinition.CronPolicy.KEEP_EXISTING;
        @JsonIgnore
        private CronTrigger<NamedJob> scheduledTrigger;
        @JsonIgnore
        private String triggerGroup = null;
        @JsonIgnore
        private String triggerId = null;
        @JsonIgnore
        private TriggerOperator triggerOperator;

        @JsonCreator
        @JsonIgnoreProperties(ignoreUnknown = true)
        public SLA(
                @JsonProperty("triggerOperator") TriggerOperator triggerOperator,
                @JsonProperty("min") int min,
                @JsonProperty("max") int max,
                @JsonProperty("cronSpec") String cronSpec,
                @JsonProperty("cronPolicy") NamedJobDefinition.CronPolicy cronPolicy
        ) {
            this.triggerOperator = triggerOperator;
            if (cronSpec != null && !cronSpec.isEmpty()) {
                this.cronSpec = cronSpec;
                hasCronSpec = true;
                this.max = 1;
                this.min = 0;
                this.cronPolicy = cronPolicy == null ? defaultPolicy : cronPolicy;
            } else {
                hasCronSpec = false;
                this.min = min;
                this.max = max;
                this.cronSpec = null;
                this.cronPolicy = null;
            }
        }

        public int getMin() {
            return min;
        }

        public int getMax() {
            return max;
        }

        public String getCronSpec() {
            return cronSpec;
        }

        public NamedJobDefinition.CronPolicy getCronPolicy() {
            return cronPolicy;
        }

        public void setTriggerOperator(TriggerOperator triggerOperator) {
            this.triggerOperator = triggerOperator;
        }

        private void validate() throws InvalidNamedJobException {
            if (max < min) {
                throw new InvalidNamedJobException("Cannot have max=" + max + " < min=" + min);
            }
            if (min > MaxValueForSlaMin) {
                throw new InvalidNamedJobException("Specified min sla value " + min + " cannot be >" + MaxValueForSlaMin);
            }
            if (max > MaxValueForSlaMax) {
                throw new InvalidNamedJobException("Max sla value " + max + " cannot be >" + MaxValueForSlaMax);
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(NamedJob.class);
    private static final int MaxValueForSlaMin = 5;
    private static final int MaxValueForSlaMax = 100;
    private final String name;
    private final List<Jar> jars = new ArrayList<>();
    private JobOwner owner;
    private volatile SLA sla;
    private List<Parameter> parameters;
    private volatile long lastJobCount = 0;
    private volatile boolean disabled = false;
    private volatile boolean cronActive = false;
    @JsonIgnore
    private Map<String, CompletedJob> completedJobs = new HashMap<>();
    @JsonIgnore
    private final BehaviorSubject<String> jobIds;
    @JsonIgnore
    private final SortedSet<V2JobMgrIntf> sortedJobMgrs;
    @JsonIgnore
    private final SortedSet<V2JobMgrIntf> sortedRegisteredJobMgrs;
    @JsonIgnore
    private final ReentrantLock lock = new ReentrantLock();
    @JsonIgnore
    private volatile boolean isActive = true;
    @JsonIgnore
    private MasterConfiguration config;
    @JsonIgnore
    private V2JobOperations jobOps;
    @JsonIgnore
    private final Comparator<V2JobMgrIntf> comparator = (o1, o2) -> {
        if (o2 == null) {
            return -1;
        }
        if (o1 == null) {
            return 1;
        }
        return Long.compare(getJobIdNumber(o1.getJobId()), getJobIdNumber(o2.getJobId()));
    };
    @JsonIgnore
    private V2StorageProvider storageProvider = null;

    @JsonCreator
    @JsonIgnoreProperties(ignoreUnknown = true)
    public NamedJob(@JsonProperty("config") MasterConfiguration config,
                    @JsonProperty("jobOps") V2JobOperations jobOps,
                    @JsonProperty("triggerOperator") TriggerOperator triggerOperator,
                    @JsonProperty("name") String name,
                    @JsonProperty("jars") List<Jar> jars, @JsonProperty("sla") SLA sla,
                    @JsonProperty("parameters") List<Parameter> parameters,
                    @JsonProperty("owner") JobOwner owner, @JsonProperty("lastJobCount") long lastJobCount,
                    @JsonProperty("disabled") boolean disabled) {
        this.config = config;
        this.jobOps = jobOps;
        this.name = name;
        if (sla == null) {
            sla = new SLA(triggerOperator, 0, 0, null, null);
        }
        this.disabled = disabled;
        this.sla = sla;
        try {
            this.sla.validate();
        } catch (InvalidNamedJobException e) {
            logger.warn(name + ": disabling due to unexpected error validating sla: " + e.getMessage());
            this.disabled = true;
        }
        this.parameters = parameters;
        this.owner = owner;
        this.lastJobCount = lastJobCount;
        this.disabled = disabled;
        if (jars != null) {
            this.jars.addAll(jars);
        }
        jobIds = BehaviorSubject.create();
        final Comparator<V2JobMgrIntf> comparator = new Comparator<V2JobMgrIntf>() {
            @Override
            public int compare(V2JobMgrIntf o1, V2JobMgrIntf o2) {
                if (o2 == null) {
                    return -1;
                }
                if (o1 == null) {
                    return 1;
                }
                return Long.compare(getJobIdNumber(o1.getJobId()), getJobIdNumber(o2.getJobId()));
            }
        };
        sortedJobMgrs = new TreeSet<>(comparator);
        sortedRegisteredJobMgrs = new TreeSet<>(comparator);
    }

    @JsonIgnore
    void setStorageProvider(V2StorageProvider storageProvider) {
        this.storageProvider = storageProvider;
    }

    private void trim() {
        if (jars.size() > config.getMaximumNumberOfJarsPerJobName()) {
            final Iterator<Jar> iterator = jars.iterator();
            int toRemove = jars.size() - config.getMaximumNumberOfJarsPerJobName();
            while (iterator.hasNext() && toRemove-- > 0) {
                final Jar next = iterator.next();
                if (notReferencedByJobs(next)) {
                    iterator.remove();
                }
            }
        }
    }

    private boolean notReferencedByJobs(Jar next) {
        for (V2JobMgrIntf jobMgr : sortedJobMgrs) {
            if (jobMgr.isActive() && jobMgr.getJobMetadata().getJarUrl().toString().equals(next.getUrl().toString())) {
                return false;
            }
        }
        return true;
    }

    /* package */ void setConfig(MasterConfiguration config) {
        this.config = config;
    }

    /* package */ void setJobOps(V2JobOperations jobOps) {
        this.jobOps = jobOps;
    }

    /* package */ void setTriggerOperator(TriggerOperator triggerOperator) {
        if (sla != null) {
            sla.setTriggerOperator(triggerOperator);
        }
    }

    public void addJar(Jar jar) throws InvalidNamedJobException {
        // add only if version is unique
        for (Jar j : jars) {
            if (j.version.equals(jar.version)) {
                throw new InvalidNamedJobException("Jar version " + jar.version + " already used, must be unique");
            }
        }
        jars.add(jar);
        trim();
    }

    public String getName() {
        return name;
    }

    public List<Jar> getJars() {
        return Collections.unmodifiableList(jars);
    }

    public SLA getSla() {
        return sla;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public JobOwner getOwner() {
        return owner;
    }

    public long getLastJobCount() {
        return lastJobCount;
    }

    @JsonIgnore
    public long getNextJobNumber() {
        return ++lastJobCount;
    }

    public boolean getDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
        enforceSla();
    }

    @JsonIgnore
    public boolean getIsActive() {
        return isActive;
    }

    @JsonIgnore
    public void setInactive() {
        isActive = false;
    }

    public static String getJobId(String name, long number) {
        return name + "-" + number;
    }

    public static String getJobName(String jobId) {
        return jobId.substring(0, jobId.lastIndexOf('-'));
    }

    private static long getJobIdNumber(String jobId) {
        return Long.parseLong(jobId.substring(jobId.lastIndexOf('-') + 1));
    }

    void setSla(SLA sla) throws InvalidNamedJobException {
        try (AutoCloseable l = obtainLock()) {
            sla.validate();
            this.sla = sla;
            enforceSla();
        } catch (Exception e) { // shouldn't happen, this is only to make obtainlock() happy
            logger.warn("Unexpected exception setting sla: " + e.getMessage());
            throw new InvalidNamedJobException("Unexpected error: " + e.getMessage(), e);
        }
    }

    void setOwner(JobOwner owner) {
        this.owner = owner;
    }

    void setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
    }

    /**
     * Get the Jar for the job that matches the <code>version</code> argument.
     *
     * @param version The version to match.
     * @return Latest jar uploaded if <code>version</code> is <code>null</code> or empty, or jar whose version
     * matches the argument, or null if no such version exists.
     */
    @JsonIgnore
    Jar getJar(String version) {
        if (version == null || version.isEmpty()) {
            return jars.get(jars.size() - 1);
        }
        for (Jar j : jars) {
            if (version.equals(j.version)) {
                return j;
            }
        }
        return null;
    }

    public void init(Collection<V2JobMgrIntf> jobMgrs) {
        logger.info("Init'ing named job " + name + " with " + (jobMgrs == null ? 0 : jobMgrs.size()) + " jobs");
        if (jobMgrs == null || jobMgrs.isEmpty()) {
            return;
        }
        for (V2JobMgrIntf m : jobMgrs) {
            if (m.getJobMetadata().getState() == V2JobState.Accepted) {
                sortedRegisteredJobMgrs.add(m);
            } else {
                sortedJobMgrs.add(m);
            }
        }
        if (!sortedJobMgrs.isEmpty()) {
            jobIds.onNext(sortedJobMgrs.last().getJobId());
        }
    }

    public void registerJobMgr(V2JobMgrIntf m) {
        try (AutoCloseable l = obtainLock()) {
            sortedRegisteredJobMgrs.add(m);
        } catch (Exception e) {
            logger.warn("Unexpected error: " + e.getMessage());
        }
    }

    void initCompletedJob(CompletedJob c) {
        if (c != null) {
            completedJobs.put(c.getJobId(), c);
        }
    }

    /* package */ void enforceSla() {
        try (AutoCloseable l = obtainLock()) {
            if (disabled) {
                List<V2JobMgrIntf> jobsToKill = new ArrayList<>();
                jobsToKill.addAll(sortedRegisteredJobMgrs);
                jobsToKill.addAll(sortedJobMgrs);
                if (!jobsToKill.isEmpty()) {
                    // ensure no job is running
                    for (V2JobMgrIntf jobMgr : jobsToKill) {
                        if (jobMgr.isActive()) {
                            jobOps.killJob("TitusMaster", jobMgr.getJobId(), "job " + getName() + " is disabled");
                        }
                    }
                }
                return;
            }
            if (sla == null || (sla.min == 0 && sla.max == 0)) {
                return;
            }
            List<V2JobMgrIntf> activeJobMgrs = new ArrayList<>();
            for (V2JobMgrIntf m : sortedJobMgrs) {
                if (m.isActive()) {
                    activeJobMgrs.add(m);
                }
            }
            List<V2JobMgrIntf> activeRgstrdJobMgrs = new ArrayList<>();
            for (V2JobMgrIntf m : sortedRegisteredJobMgrs) {
                if (m.isActive()) {
                    activeRgstrdJobMgrs.add(m);
                }
            }
            // there could be some jobs running and some registered but not running yet. Eagerly enforcing the sla.max
            // could result in killing the running job in favor of the new job, which may not start successfully. Instead,
            // we take the following approach:
            // Manage min by combining the total of both running and registered jobs. This ensures we don't start
            // too many new jobs if previously started ones stay in registered for too long for not successfully starting.
            if (sla != null && !sortedJobMgrs.isEmpty() && (activeJobMgrs.size() + activeRgstrdJobMgrs.size()) < sla.min) {
                logger.info("Submitting " + (sla.min - activeJobMgrs.size()) + " jobs per sla min of " + sla.min +
                        " for job name " + name);
                for (int i = 0; i < sla.min - activeJobMgrs.size(); i++) {
                    V2JobMetadata last = null;
                    if (!sortedJobMgrs.isEmpty()) {
                        last = sortedJobMgrs.last().getJobMetadata();
                    }
                    if (last == null) {
                        // get it from archived jobs
                        if (!completedJobs.isEmpty()) {
                            long latestCompletedAt = 0L;
                            CompletedJob latest = null;
                            for (CompletedJob j : completedJobs.values()) {
                                if (latest == null || latestCompletedAt < j.getTerminatedAt()) {
                                    latest = j;
                                    latestCompletedAt = j.getTerminatedAt();
                                }
                            }
                            if (latest != null) {
                                last = storageProvider.loadArchivedJob(latest.getJobId());
                            }
                        }
                    }
                    if (last == null) {
                        logger.warn("Can't submit new job to maintain sla for job cluster " + name + ": no previous job to clone");
                    } else {
                        submitNewJob(last);
                    }
                }
            }
            // Manage max by killing any excess running jobs. Also, kill any registered jobs older than remaining
            // running jobs, or in excess of sla.max.
            // For this we sort running and registered JobMgrs and walk the list in descending order to apply this logic.
            SortedSet<V2JobMgrIntf> allSortedJobMgrs = new TreeSet<>(comparator);
            allSortedJobMgrs.addAll(activeJobMgrs);
            allSortedJobMgrs.addAll(activeRgstrdJobMgrs);
            final V2JobMgrIntf[] jobMgrs = allSortedJobMgrs.toArray(new V2JobMgrIntf[allSortedJobMgrs.size()]);
            if (jobMgrs.length > 0) {
                boolean slaSatisfied = false;
                int activeCount = 0;
                int registeredCount = 0;
                int killedCount = 0;
                for (int i = jobMgrs.length - 1; i >= 0; i--) {
                    V2JobMgrIntf m = jobMgrs[i];
                    boolean isActive = m.getJobMetadata() != null &&
                            m.getJobMetadata().getState() == V2JobState.Launched;
                    if (!isActive) {
                        registeredCount++;
                    }
                    if (!isActive && !slaSatisfied && (registeredCount + activeCount) <= sla.max) {
                        continue;
                    }
                    if (slaSatisfied || (!isActive && (registeredCount + activeCount) > sla.max)) {
                        slaKill(m);
                        killedCount++;
                    } else if (isActive) {
                        activeCount++;
                    }
                    if (activeCount >= sla.max) {
                        slaSatisfied = true;
                    }
                }
                if (killedCount > 0) {
                    logger.info(name + ": killed " + killedCount + " jobs per sla max of " + sla.max);
                }
            }
        } catch (Exception e) {
            logger.error("Unknown error enforcing SLA for " + name + ": " + e.getMessage(), e);
        } // shouldn't happen
        finally {
            removeExpiredCompletedJobs();
        }
    }

    private void removeExpiredCompletedJobs() {
        if (!completedJobs.isEmpty()) {
            final long cutOff = System.currentTimeMillis() - (config.getTerminatedJobToDeleteDelayHours() * 3600000L);
            for (CompletedJob j : new LinkedList<>(completedJobs.values())) {
                if (j.getTerminatedAt() < cutOff) {
                    try {
                        storageProvider.removeCompledtedJobForNamedJob(name, j.getJobId());
                        completedJobs.remove(j.getJobId());
                    } catch (IOException e) {
                        logger.warn("Error removing completed job " + j.getJobId() + ": " + e.getMessage(), e);
                    }
                }
            }
        }
    }

    private void slaKill(V2JobMgrIntf jobMgr) {
        jobOps.killJob("TitusMaster", jobMgr.getJobId(), "#jobs exceeded for SLA max of " + sla.max);
    }

    String quickSubmit(String user) throws InvalidJobException {
        final V2JobMgrIntf jobMgr = sortedJobMgrs.isEmpty() ? null : sortedJobMgrs.last();
        if (jobMgr == null) {
            throw new InvalidJobException("No previous job to copy parameters or scheduling info for quick submit");
        }
        return submitNewJob(jobMgr.getJobMetadata(), user);
    }

    private String submitNewJob(V2JobMetadata last) {
        return submitNewJob(last, last.getUser());
    }

    private String submitNewJob(V2JobMetadata last, String user) {
        try (AutoCloseable l = obtainLock()) {
            try {
                return jobOps.submit(new V2JobDefinition(name, user,
                        null, // don't specify jar, let it pick latest
                        null, // don't specify jar version, let it pick latest
                        last.getParameters(),
                        last.getSla(),
                        last.getSubscriptionTimeoutSecs(),
                        V2JobStore.getSchedulingInfo(last), sla.min, sla.max, sla.cronSpec, sla.cronPolicy));
            } catch (IllegalArgumentException e) {
                logger.error("Couldn't submit replacement job for " + name + " - " + e.getMessage(), e);
                return null;
            }
        } catch (Exception e) {
            // shouldn't happen
            logger.error("Unexpected error obtaining lock: " + e.getMessage(), e);
            return null;
        }
    }

    public void removeJobMgr(V2JobMgrIntf m, String jobId) {
        if (m != null) {
            logger.info("Removing job " + m.getJobId());
            try (AutoCloseable l = obtainLock()) {
                sortedRegisteredJobMgrs.remove(m);
                sortedJobMgrs.remove(m);
            } catch (Exception e) {
            } // shouldn't happen
            enforceSla();
        }
        completedJobs.remove(jobId);
    }

    public String submitWithLatestJar() {
        try (AutoCloseable l = obtainLock()) {
            if (sortedJobMgrs.isEmpty()) {
                return null;
            }
            final String jobId = submitNewJob(sortedJobMgrs.last().getJobMetadata());
            if (jobId != null) {
                return jobId;
            }
        } catch (Exception e) {
        } // shouldn't happen
        return null;
    }

    /**
     * Obtain a lock on this object, All operations on this object work without checking for concurrent updates. Callers
     * are expected to call this method to lock this object for safe modifications and unlock after use. The return object
     * can be used in try with resources for reliable unlocking.
     *
     * @return {@link java.lang.AutoCloseable} lock object.
     */
    public AutoCloseable obtainLock() {
        lock.lock();
        return new AutoCloseable() {
            @Override
            public void close() throws IllegalMonitorStateException {
                lock.unlock();
            }
        };
    }

    static boolean isValidJobName(String name) {
        return VALID_JOB_ID_RE.matcher(name).matches();
    }
}
