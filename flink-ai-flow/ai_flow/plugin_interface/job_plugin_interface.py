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
from typing import Text
import logging
from ai_flow.common.registry import BaseRegistry
from ai_flow.common.json_utils import Jsonable
from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow.project.project_description import ProjectDesc
from ai_flow.translator.translator import register_job_generator
from ai_flow.translator.base_translator import BaseJobGenerator
from ai_flow.workflow.job import Job


class BaseJobHandler(Jsonable):
    def __init__(self,
                 job: Job,
                 job_execution: JobExecutionInfo) -> None:
        self.job: Job = job
        self.job_execution: JobExecutionInfo = job_execution


class BaseJobSubmitter(ABC):
    """
    Used for submitting an executable job to specific platform when it's scheduled to run in workflow scheduler.
    The submitter is able to control the lifecycle of a workflow job. Users can also implement custom job submitter with
    their own job engine and execution platform.
    """

    def __init__(self) -> None:
        """
        Construct a :py:class:`ai_flow.deployer.job_submitter.BaseJobSubmitter`
        """
        pass

    @abstractmethod
    def submit_job(self, job: Job, project_desc: ProjectDesc) -> BaseJobHandler:
        """
        submit an executable job to run.
        :param job: A job object that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        :return base_job_handler: a job handler that maintain the handler of a jobs runtime.
        """
        pass

    @abstractmethod
    def stop_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        """
        Stop a ai flow job.
        :param job_handler: The job handler that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        """
        pass

    @abstractmethod
    def cleanup_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        """
        clean up temporary resources created during this execution.
        :param job_handler: The job handler that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        """
        pass


class JobSubmitterManager(BaseRegistry):
    def __init__(self) -> None:
        super().__init__()

    def submit_job(self, job: Job, project_desc: ProjectDesc) -> BaseJobHandler:
        job_submitter = self.get_job_submitter(job)
        return job_submitter.submit_job(job, project_desc)

    def get_job_submitter(self, job: Job)->BaseJobSubmitter:
        job_submitter: BaseJobSubmitter = self.get_object(job.job_config.job_type)
        if job_submitter is None:
            raise Exception("job submitter not found! job_type {}".format(job.job_config.job_type))
        return job_submitter

    def stop_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        job_submitter = self.get_job_submitter(job_handler.job)
        job_submitter.stop_job(job_handler, project_desc)

    def cleanup_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        job_submitter = self.get_job_submitter(job_handler.job)
        job_submitter.cleanup_job(job_handler, project_desc)


__default_job_submitter_manager__ = JobSubmitterManager()


def register_job_submitter(job_type: Text, job_submitter: BaseJobSubmitter):
    __default_job_submitter_manager__.register(job_type, job_submitter)


def get_default_job_submitter_manager() -> JobSubmitterManager:
    return __default_job_submitter_manager__


class AbstractJobPlugin(BaseJobGenerator, BaseJobSubmitter):

    def __init__(self) -> None:
        super().__init__()

    @abstractmethod
    def generate(self, sub_graph: AISubGraph, project_desc: ProjectDesc) -> Job:
        """
        Generate the job to execute.
        :param sub_graph: The sub graph of AI graph, which describes the operation logic of a job
        :param project_desc: The ai flow project description.
        :return: the job.
        """
        pass

    @abstractmethod
    def generate_job_resource(self, job: Job, project_desc: ProjectDesc) -> None:
        """
        :param job: A base job object that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        :return None
        """
        pass

    @abstractmethod
    def submit_job(self, job: Job, project_desc: ProjectDesc) -> BaseJobHandler:
        """
        submit an executable job to run.
        :param job: A base job object that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        :return base_job_handler: a job handler that maintain the handler of a jobs runtime.
        """
        pass

    @abstractmethod
    def stop_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        """
        Stop a ai flow job.
        :param job_handler: The job handler that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        """
        pass

    @abstractmethod
    def cleanup_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc):
        """
        clean up temporary resources created during this execution.
        :param job_handler: The job handler that contains the necessary information for an execution.
        :param project_desc: The ai flow project description.
        """
        pass

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
