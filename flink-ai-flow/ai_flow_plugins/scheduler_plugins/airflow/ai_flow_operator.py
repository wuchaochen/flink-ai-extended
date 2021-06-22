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
from ai_flow.common.module_load import import_string
from ai_flow.plugin_interface.blob_manager_interface import BlobManagerFactory
from ai_flow.plugin_interface.job_plugin_interface import BaseJobSubmitter, JobHandler, JobExecutionContext
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo, WorkflowExecutionInfo, WorkflowInfo
from ai_flow.project.project_description import get_project_description_from
from ai_flow.runtime.job_runtime_env import JobRuntimeEnv
from ai_flow.workflow.job import Job
from ai_flow.workflow.workflow import Workflow, WorkflowPropertyKeys
from ai_flow.project.project_description import ProjectDesc


class AIFlowOperator(BaseOperator):
    @apply_defaults
    def __init__(
            self,
            job: Job,
            workflow: Workflow,
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.job: Job = job
        self.workflow: Workflow = workflow
        plugins = self.workflow.properties.get(WorkflowPropertyKeys.JOB_PLUGINS)
        module, name = plugins.get(self.job.job_config.job_type)
        class_object = import_string('{}.{}'.format(module, name))
        self.job_submitter: BaseJobSubmitter = class_object()
        self.job_handler: JobHandler = None
        self.job_context: JobExecutionContext = None

    def context_to_job_info(self, project_name: Text, context: Any)->JobExecutionInfo:
        wi = WorkflowInfo(namespace=project_name, workflow_name=self.workflow.workflow_name)
        we = WorkflowExecutionInfo(workflow_execution_id=context.get('dag_run').run_id,
                                   workflow_info=wi,
                                   state=context.get('dag_run').get_state())
        je = JobExecutionInfo(job_name=self.job.job_name,
                              job_execution_id=str(context.get('ti').try_number),
                              state=context.get('ti').state,
                              workflow_execution=we)
        return je

    def prepare_job_runtime_env(self, workflow_id, workflow_name, job_name, project_desc: ProjectDesc)->JobRuntimeEnv:
        temp_path = project_desc.get_absolute_temp_path()
        working_dir = os.path.join(temp_path, workflow_id, job_name)
        job_runtime_env: JobRuntimeEnv = JobRuntimeEnv(working_dir=working_dir,
                                                       workflow_name=workflow_name,
                                                       job_name=job_name)
        if not os.path.exists(working_dir):
            os.makedirs(job_runtime_env.log_dir)
            job_runtime_env.save_workflow_name()
            job_runtime_env.save_job_name()
            os.symlink(project_desc.get_absolute_workflow_entry_file(workflow_name=workflow_name),
                       os.path.join(working_dir, '{}.py'.format(workflow_name)))
            os.symlink(project_desc.get_absolute_workflow_config_file(workflow_name=workflow_name),
                       os.path.join(working_dir, '{}.yaml'.format(workflow_name)))
            os.symlink(os.path.join(project_desc.get_absolute_generated_path(), workflow_id, job_name),
                       job_runtime_env.generated_dir)
            os.symlink(project_desc.get_absolute_resources_path(), job_runtime_env.resource_dir)
            os.symlink(project_desc.get_absolute_dependencies_path(), job_runtime_env.dependencies_dir)
            os.symlink(project_desc.get_absolute_project_config_file(), job_runtime_env.project_config_file)

        return job_runtime_env

    def pre_execute(self, context: Any):
        config = {}
        config.update(self.workflow.properties['blob'])
        local_repo = config.get('local_repository')
        if local_repo is not None:
            # Maybe Download the project code
            if not os.path.exists(local_repo):
                os.makedirs(local_repo)
            blob_manager = BlobManagerFactory.get_blob_manager(config)
            project_path: Text = blob_manager \
                .download_blob(workflow_id=self.workflow.workflow_id,
                               remote_path=self.workflow.project_uri,
                               local_path=local_repo)
        else:
            project_path = self.workflow.project_uri
        self.log.info("project_path:" + project_path)
        project_desc = get_project_description_from(project_path)

        job_execution_info: JobExecutionInfo = self.context_to_job_info(project_desc.project_name, context)
        job_runtime_env = self.prepare_job_runtime_env(self.workflow.workflow_id,
                                                       self.workflow.workflow_name,
                                                       job_execution_info.job_name,
                                                       project_desc)
        self.job_context: JobExecutionContext \
            = JobExecutionContext(job_runtime_env=job_runtime_env,
                                  project_config=project_desc.project_config,
                                  workflow_config=self.workflow.workflow_config,
                                  job_execution_info=job_execution_info)

    def execute(self, context: Any):
        self.log.info("context:" + str(context))
        self.job_handler: JobHandler = self.job_submitter.submit_job(self.job, self.job_context)
        self.job_handler.wait_finished()
        result = self.job_handler.get_result()
        self.job_submitter.cleanup_job(self.job_handler, self.job_context)
        return result

    def on_kill(self):
        self.job_submitter.stop_job(self.job_handler, self.job_context)
        self.job_submitter.cleanup_job(self.job_handler, self.job_context)
