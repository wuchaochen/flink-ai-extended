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
from typing import Text, Dict, Optional, List

from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import AbstractJobPlugin, register_job_plugin, JobHandler, \
    JobExecutionContext
from ai_flow.workflow.job import Job
from ai_flow.plugin_interface.blob_manager_interface import BlobManager
from ai_flow.plugin_interface.scheduler_interface import AbstractScheduler, JobExecutionInfo, WorkflowExecutionInfo, \
    WorkflowInfo

from ai_flow.context.project_context import ProjectContext
from ai_flow.workflow.state import State
from ai_flow.workflow.workflow import Workflow


class MockBlobManger(BlobManager):
    def __init__(self, config):
        super().__init__(config)

    def upload_blob(self, workflow_id: Text, prj_pkg_path: Text) -> Text:
        return prj_pkg_path

    def download_blob(self, workflow_id, remote_path: Text, local_path: Text = None) -> Text:
        return remote_path


class MockJob(AbstractJobPlugin):

    def job_type(self) -> Text:
        return 'mock'

    def generate(self, sub_graph: AISubGraph, resource_dir: Text = None) -> Job:
        return Job(job_config=sub_graph.config)

    def submit_job(self, job: Job, job_context: JobExecutionContext) -> JobHandler:
        pass

    def stop_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        pass

    def cleanup_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        pass


class MockScheduler(AbstractScheduler):

    def submit_workflow(self, workflow: Workflow, project_desc: ProjectContext, args: Dict = None) -> WorkflowInfo:
        return WorkflowInfo(namespace=project_desc.project_name, workflow_name=workflow.workflow_name)

    def delete_workflow(self, project_name: Text, workflow_name: Text) -> Optional[WorkflowInfo]:
        return WorkflowInfo(namespace=project_name, workflow_name=workflow_name)

    def pause_workflow_scheduling(self, project_name: Text, workflow_name: Text) -> WorkflowInfo:
        return WorkflowInfo(namespace=project_name, workflow_name=workflow_name)

    def resume_workflow_scheduling(self, project_name: Text, workflow_name: Text) -> WorkflowInfo:
        return WorkflowInfo(namespace=project_name, workflow_name=workflow_name)

    def get_workflow(self, project_name: Text, workflow_name: Text) -> Optional[WorkflowInfo]:
        return WorkflowInfo(namespace=project_name, workflow_name=workflow_name)

    def list_workflows(self, project_name: Text) -> List[WorkflowInfo]:
        return [WorkflowInfo(namespace=project_name, workflow_name='workflow_1'),
                WorkflowInfo(namespace=project_name, workflow_name='workflow_2')]

    def start_new_workflow_execution(self, project_name: Text, workflow_name: Text) -> Optional[WorkflowExecutionInfo]:
        return WorkflowExecutionInfo(workflow_execution_id='1', state=State.RUNNING)

    def kill_all_workflow_execution(self, project_name: Text, workflow_name: Text) -> List[WorkflowExecutionInfo]:
        return [WorkflowExecutionInfo(workflow_execution_id='1', state=State.RUNNING),
                WorkflowExecutionInfo(workflow_execution_id='2', state=State.RUNNING)]

    def kill_workflow_execution(self, execution_id: Text) -> Optional[WorkflowExecutionInfo]:
        return WorkflowExecutionInfo(workflow_execution_id='1', state=State.RUNNING)

    def get_workflow_execution(self, execution_id: Text) -> Optional[WorkflowExecutionInfo]:
        return WorkflowExecutionInfo(workflow_execution_id='1', state=State.RUNNING)

    def list_workflow_executions(self, project_name: Text, workflow_name: Text) -> List[WorkflowExecutionInfo]:
        return [WorkflowExecutionInfo(workflow_execution_id='1', state=State.RUNNING),
                WorkflowExecutionInfo(workflow_execution_id='2', state=State.RUNNING)]

    def start_job_execution(self, job_name: Text, execution_id: Text) -> JobExecutionInfo:
        return JobExecutionInfo(job_name='task_1', state=State.RUNNING)

    def stop_job_execution(self, job_name: Text, execution_id: Text) -> JobExecutionInfo:
        return JobExecutionInfo(job_name='task_1', state=State.RUNNING)

    def restart_job_execution(self, job_name: Text, execution_id: Text) -> JobExecutionInfo:
        return JobExecutionInfo(job_name='task_1', state=State.RUNNING)

    def get_job_executions(self, job_name: Text, execution_id: Text) -> List[JobExecutionInfo]:
        return [JobExecutionInfo(job_name='task_1', state=State.RUNNING)]

    def list_job_executions(self, execution_id: Text) -> List[JobExecutionInfo]:
        return [JobExecutionInfo(job_name='task_1', state=State.RUNNING),
                JobExecutionInfo(job_name='task_2', state=State.RUNNING)]


register_job_plugin(MockJob())
