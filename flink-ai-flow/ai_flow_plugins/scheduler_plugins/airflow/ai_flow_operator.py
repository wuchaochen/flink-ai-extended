# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from typing import Any, Text
import os
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from ai_flow.plugin_interface.blob_manager_interface import BlobManagerFactory
from ai_flow.plugin_interface.job_plugin_interface import BaseJobSubmitter, BaseJobHandler
from ai_flow.project.project_description import ProjectDesc, get_project_description_from
from ai_flow.workflow.job import Job
from ai_flow.workflow.workflow import Workflow


class AIFlowOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            job: Job,
            workflow: Workflow,
            local_repo: Text = None,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job: Job = job
        self.workflow: Workflow = workflow
        self.job_submitter: BaseJobSubmitter = None
        self.job_handler: BaseJobHandler = None
        self.project_desc: ProjectDesc = None
        self.local_repo: Text = local_repo

    def pre_execute(self, context: Any):
        if not os.path.exists(self.local_repo):
            os.makedirs(self.local_repo)
        # Maybe Download the project code
        config = {}
        config.update(self.workflow.properties['blob'])
        blob_manager = BlobManagerFactory.get_blob_manager(config)
        project_path: Text = blob_manager \
            .download_blob(workflow_id=self.workflow.workflow_id,
                           remote_path=self.workflow.project_uri,
                           local_path=self.local_repo)
        self.project_desc = get_project_description_from(project_path)

    def execute(self, context: Any):
        print(context)
        self.job_handler: BaseJobHandler = self.job_submitter.submit_job(self.job, self.project_desc)

    def on_kill(self):
        self.job_submitter.stop_job(self.job_handler, self.project_desc)
