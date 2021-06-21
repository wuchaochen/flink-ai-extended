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
from abc import abstractmethod, ABC
from typing import Text, Dict
import os
import logging
from ai_flow.common.registry import BaseRegistry
from ai_flow.common.json_utils import Jsonable
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow.project.project_config import ProjectConfig
from ai_flow.workflow.workflow_config import WorkflowConfig
from ai_flow.translator.translator import register_job_generator
from ai_flow.translator.base_translator import BaseJobGenerator
from ai_flow.workflow.job import Job


class JobHandler(Jsonable):
    def __init__(self,
                 job: Job,
                 job_execution: JobExecutionInfo) -> None:
        self._log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        self.job: Job = job
        self.job_execution: JobExecutionInfo = job_execution

    def get_result(self)->object:
        pass

    def wait_finished(self):
        pass

    @property
    def log(self) -> logging.Logger:
        """Returns a logger."""
        return self._log


class JobRuntimeEnv(object):
    def __init__(self,
                 working_dir: Text):
        self._working_dir: Text = working_dir

    @property
    def working_dir(self)->Text:
        return self._working_dir

    @property
    def project_path(self)->Text:
        return os.path.dirname(os.path.dirname(os.path.dirname(self._working_dir)))

    @property
    def log_dir(self) -> Text:
        return os.path.join(self._working_dir, 'logs')

    @property
    def resource_dir(self)->Text:
        return os.path.join(self._working_dir, 'resources')

    @property
    def generated_dir(self)->Text:
        return os.path.join(self._working_dir, 'generated')

    @property
    def dependencies_dir(self)->Text:
        return os.path.join(self._working_dir, 'dependencies')

    @property
    def python_dep_dir(self)->Text:
        return os.path.join(self.dependencies_dir, 'python')

    @property
    def go_dep_dir(self) -> Text:
        return os.path.join(self.dependencies_dir, 'go')

    @property
    def jar_dep_dir(self) -> Text:
        return os.path.join(self.dependencies_dir, 'jar')


class JobExecutionContext(object):
    def __init__(self,
                 job_runtime_env: JobRuntimeEnv,
                 project_config: ProjectConfig,
                 workflow_config: WorkflowConfig,
                 job_execution_info: JobExecutionInfo):
        self.job_runtime_env: JobRuntimeEnv = job_runtime_env
        self.project_config: ProjectConfig = project_config
        self.workflow_config: WorkflowConfig = workflow_config
        self.job_execution_info: JobExecutionInfo = job_execution_info


class BaseJobSubmitter(ABC):
    """
    Used for submitting an executable job to specific platform when it's scheduled to run in workflow scheduler.
    The submitter is able to control the lifecycle of a workflow job. Users can also implement custom job submitter with
    their own job engine and execution platform.
    """

    def __init__(self) -> None:
        self._log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)

    @property
    def log(self) -> logging.Logger:
        """Returns a logger."""
        return self._log

    @abstractmethod
    def submit_job(self, job: Job, job_context: JobExecutionContext) -> JobHandler:
        """
        submit an executable job to run.
        :param job_context:
        :param job: A job object that contains the necessary information for an execution.
        :return base_job_handler: a job handler that maintain the handler of a jobs runtime.
        """
        pass

    @abstractmethod
    def stop_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        """
        Stop a ai flow job.
        :param job_context:
        :param job_handler: The job handler that contains the necessary information for an execution.
        """
        pass

    @abstractmethod
    def cleanup_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        """
        clean up temporary resources created during this execution.
        :param job_context:
        :param job_handler: The job handler that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        """
        pass


class JobSubmitterManager(BaseRegistry):
    def __init__(self) -> None:
        super().__init__()

    def submit_job(self, job: Job, job_context: JobExecutionContext) -> JobHandler:
        job_submitter = self.get_job_submitter(job)
        return job_submitter.submit_job(job, job_context)

    def get_job_submitter(self, job: Job)->BaseJobSubmitter:
        job_submitter: BaseJobSubmitter = self.get_object(job.job_config.job_type)
        if job_submitter is None:
            raise Exception("job submitter not found! job_type {}".format(job.job_config.job_type))
        return job_submitter

    def stop_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        job_submitter = self.get_job_submitter(job_handler.job)
        job_submitter.stop_job(job_handler, job_context)

    def cleanup_job(self, job_handler: JobHandler, job_context: JobExecutionContext):
        job_submitter = self.get_job_submitter(job_handler.job)
        job_submitter.cleanup_job(job_handler, job_context)


__default_job_submitter_manager__ = JobSubmitterManager()


def register_job_submitter(job_type: Text, job_submitter: BaseJobSubmitter):
    __default_job_submitter_manager__.register(job_type, job_submitter)


def get_default_job_submitter_manager() -> JobSubmitterManager:
    return __default_job_submitter_manager__


class AbstractJobPlugin(BaseJobGenerator, BaseJobSubmitter):

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def job_type(self) -> Text:
        """
        :return: The job type.
        """
        pass


def register_job_plugin(plugin: AbstractJobPlugin):
    logging.debug("Register job plugin {} {}".format(plugin.__class__.__name__, plugin.job_type()))
    register_job_generator(job_type=plugin.job_type(), generator=plugin)
    register_job_submitter(job_type=plugin.job_type(), job_submitter=plugin)


def get_registered_job_plugins()-> Dict:
    result = {}
    jm = get_default_job_submitter_manager()
    for job_type, plugin in jm.object_dict.items():
        result[job_type] = (plugin.__class__.__module__, plugin.__class__.__name__)
    return result
