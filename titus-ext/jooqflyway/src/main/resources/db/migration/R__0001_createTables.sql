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

CREATE SCHEMA IF NOT EXISTS jobactivity;


CREATE TABLE IF NOT EXISTS jobactivity.jobs
(
  job_id          VARCHAR (64) NOT NULL,
  create_time     TIMESTAMP NOT NULL,
  record_time     TIMESTAMP NOT NULL,
  job_record_json jsonb,
  PRIMARY KEY (job_id)
);


CREATE TABLE IF NOT EXISTS jobactivity.tasks
(
    task_id          VARCHAR (64) NOT NULL,
    create_time      TIMESTAMP NOT NULL,
    record_time      TIMESTAMP NOT NULL,
    task_record_json jsonb,
    PRIMARY KEY (task_id)
);