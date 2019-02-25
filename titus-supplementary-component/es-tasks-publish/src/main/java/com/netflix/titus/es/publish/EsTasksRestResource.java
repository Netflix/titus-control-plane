/*
 * Copyright 2019 Netflix, Inc.
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
package com.netflix.titus.es.publish;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EsTasksRestResource {
    private static final Logger logger = LoggerFactory.getLogger(EsTasksRestResource.class);
    private TasksPublisherCtrl tasksPublisherCtrl;

    public static class PublishedTaskSummary {
        private int numErrors;
        private int totalTasksIndexed;
        private int totalIndexUpdates;

        public int getTotalIndexUpdates() {
            return totalIndexUpdates;
        }

        public void setTotalIndexUpdates(int totalIndexUpdates) {
            this.totalIndexUpdates = totalIndexUpdates;
        }

        public int getNumErrors() {
            return numErrors;
        }

        public void setNumErrors(int numErrors) {
            this.numErrors = numErrors;
        }

        public int getTotalTasksIndexed() {
            return totalTasksIndexed;
        }

        public void setTotalTasksIndexed(int totalTasksIndexed) {
            this.totalTasksIndexed = totalTasksIndexed;
        }
    }


    @Autowired
    public EsTasksRestResource(TasksPublisherCtrl tasksPublisherCtrl) {
        this.tasksPublisherCtrl = tasksPublisherCtrl;
    }


    @RequestMapping("/es/tasks")
    public PublishedTaskSummary getSummary() {
        final PublishedTaskSummary publishedTaskSummary = new PublishedTaskSummary();
        publishedTaskSummary.numErrors = tasksPublisherCtrl.getNumErrors().get();
        publishedTaskSummary.totalTasksIndexed = tasksPublisherCtrl.getNumTasksUpdated().get();
        publishedTaskSummary.totalIndexUpdates = tasksPublisherCtrl.getNumIndexUpdated().get();
        return publishedTaskSummary;
    }

}
