/*
 * Copyright 2021 Netflix, Inc.
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

package com.netflix.titus.testkit.embedded.cloud.connector.local;

import java.util.Collection;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

public class NoOpMesosSchedulerDriver implements SchedulerDriver {
    @Override
    public Protos.Status start() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status stop(boolean failover) {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status stop() {
        return Protos.Status.DRIVER_STOPPED;
    }

    @Override
    public Protos.Status abort() {
        return Protos.Status.DRIVER_ABORTED;
    }

    @Override
    public Protos.Status join() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status run() {
        return Protos.Status.DRIVER_RUNNING;
    }

    @Override
    public Protos.Status requestResources(Collection<Protos.Request> requests) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status launchTasks(Collection<Protos.OfferID> offerIds, Collection<Protos.TaskInfo> tasks) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks, Protos.Filters filters) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status launchTasks(Protos.OfferID offerId, Collection<Protos.TaskInfo> tasks) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status killTask(Protos.TaskID taskId) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status acceptOffers(Collection<Protos.OfferID> offerIds, Collection<Protos.Offer.Operation> operations, Protos.Filters filters) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status declineOffer(Protos.OfferID offerId, Protos.Filters filters) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status declineOffer(Protos.OfferID offerId) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status reviveOffers() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status suppressOffers() {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status acknowledgeStatusUpdate(Protos.TaskStatus status) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status sendFrameworkMessage(Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {
        throw new IllegalStateException("method not implemented");
    }

    @Override
    public Protos.Status reconcileTasks(Collection<Protos.TaskStatus> statuses) {
        throw new IllegalStateException("method not implemented");
    }
}
