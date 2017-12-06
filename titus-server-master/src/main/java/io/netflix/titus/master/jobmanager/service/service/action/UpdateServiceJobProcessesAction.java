package io.netflix.titus.master.jobmanager.service.service.action;

import io.netflix.titus.api.jobmanager.model.job.ServiceJobProcesses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import io.netflix.titus.api.jobmanager.model.event.JobManagerEvent;
import io.netflix.titus.api.jobmanager.model.job.Job;
import io.netflix.titus.api.jobmanager.model.job.JobDescriptor;
import io.netflix.titus.api.jobmanager.model.job.ext.ServiceJobExt;
import io.netflix.titus.api.jobmanager.service.common.action.ActionKind;
import io.netflix.titus.api.jobmanager.service.common.action.JobChange;
import io.netflix.titus.api.jobmanager.service.common.action.TitusChangeAction;
import io.netflix.titus.common.framework.reconciler.ModelUpdateAction;
import io.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import io.netflix.titus.common.util.tuple.Pair;
import io.netflix.titus.master.jobmanager.service.common.action.TitusModelUpdateActions;
import rx.Observable;


public class UpdateServiceJobProcessesAction extends TitusChangeAction {
    private static Logger logger = LoggerFactory.getLogger(UpdateServiceJobProcessesAction.class);

    private final ReconciliationEngine engine;
    private ServiceJobProcesses serviceJobProcesses;

    public UpdateServiceJobProcessesAction(ReconciliationEngine engine, ServiceJobProcesses serviceJobProcesses) {
        super(new JobChange(ActionKind.Job, JobManagerEvent.Trigger.API, engine.getReferenceView().getId(), "Job resize operation requested"));
        this.engine = engine;
        this.serviceJobProcesses = serviceJobProcesses;
    }

    @Override
    public Observable<Pair<JobChange, List<ModelUpdateAction>>> apply() {
        Job<ServiceJobExt> job = engine.getReferenceView().getEntity();

        JobDescriptor<ServiceJobExt> jobDescriptor = job.getJobDescriptor().toBuilder()
                .withExtensions(job.getJobDescriptor().getExtensions().toBuilder()
                        .withServiceJobProcesses(serviceJobProcesses)
                        .build()
                )
                .build();

        Job<ServiceJobExt> updatedJob = job.toBuilder().withJobDescriptor(jobDescriptor).build();

        return Observable.just(Pair.of(
                getChange(),
                Collections.singletonList(
                        TitusModelUpdateActions.updateJob(
                                updatedJob,
                                JobManagerEvent.Trigger.API,
                                ModelUpdateAction.Model.Reference,
                                "Service Job Processes Update requested"
                        )
                )
        ));
    }
}
