-- /*
--  * Copyright 2019 Netflix, Inc.
--  *
--  * Licensed under the Apache License, Version 2.0 (the "License");
--  * you may not use this file except in compliance with the License.
--  * You may obtain a copy of the License at
--  *
--  *     http://www.apache.org/licenses/LICENSE-2.0
--  *
--  * Unless required by applicable law or agreed to in writing, software
--  * distributed under the License is distributed on an "AS IS" BASIS,
--  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--  * See the License for the specific language governing permissions and
--  * limitations under the License.
--  */

CREATE SCHEMA IF NOT EXISTS "relocation";

CREATE TABLE "relocation"."relocation_plan"
(
    task_id         character varying(64)       NOT NULL UNIQUE,
    reason_code     character varying(64)       NOT NULL,
    reason_message  character varying(2048)     NOT NULL,
    decision_time   timestamp without time zone NOT NULL,
    relocation_time timestamp without time zone NOT NULL,
    CONSTRAINT pk_relocation_plan_task_id PRIMARY KEY (task_id)
);

CREATE TABLE "relocation"."relocation_status"
(
    task_id                   character varying(64)       NOT NULL UNIQUE,
    relocation_state          character varying(64)       NOT NULL,
    status_code               character varying(64)       NOT NULL,
    status_message            character varying(2048)     NOT NULL,
    reason_code               character varying(64)       NOT NULL,
    reason_message            character varying(2048)     NOT NULL,
    relocation_decision_time  timestamp without time zone NOT NULL,
    relocation_plan_time      timestamp without time zone NOT NULL,
    relocation_execution_time timestamp without time zone NOT NULL,
    CONSTRAINT pk_relocation_status_task_id PRIMARY KEY (task_id)
);

CREATE INDEX relocation_plan_task_id_index ON "relocation"."relocation_plan" (task_id);
CREATE INDEX relocation_status_task_id_index ON "relocation"."relocation_status" (task_id);
