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

package com.netflix.titus.common.framework.reconciler.internal;

import java.util.List;

import com.netflix.titus.common.framework.reconciler.ChangeAction;
import com.netflix.titus.common.framework.reconciler.EntityHolder;
import com.netflix.titus.common.framework.reconciler.ReconciliationEngine;
import rx.Observable;
import rx.subjects.PublishSubject;

class EngineStub implements ReconciliationEngine<SimpleReconcilerEvent> {

    private final String id;
    private final EntityHolder referenceView;
    private final PublishSubject<SimpleReconcilerEvent> eventSubject = PublishSubject.create();

    EngineStub(String id) {
        this.id = id;
        this.referenceView = EntityHolder.newRoot(id, "initial");
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction changeAction) {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public Observable<Void> changeReferenceModel(ChangeAction changeAction, String entityHolderId) {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public EntityHolder getReferenceView() {
        return referenceView;
    }

    @Override
    public EntityHolder getRunningView() {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public EntityHolder getStoreView() {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public <ORDER_BY> List<EntityHolder> orderedView(ORDER_BY orderingCriteria) {
        throw new IllegalStateException("not implemented yet");
    }

    @Override
    public Observable<SimpleReconcilerEvent> events() {
        return eventSubject;
    }

    public void emitChange(String... changes) {
        for(String change: changes) {
            eventSubject.onNext(SimpleReconcilerEvent.newChange(change));
        }
    }
}
