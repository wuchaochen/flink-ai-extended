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
from abc import ABC
from typing import Text

from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import AbstractJobPluginFactory, JobHandler, JobRuntimeEnv, \
    JobController
from ai_flow.translator.translator import JobGenerator
from ai_flow.workflow.job import Job


class DummyJobPluginFactory(AbstractJobPluginFactory, JobGenerator, JobController):

    def __init__(self) -> None:
        super().__init__()

    def generate(self, sub_graph: AISubGraph, resource_dir: Text = None) -> Job:
        job = Job(job_config=sub_graph.config)
        return job

    def submit_job(self, job: Job, job_runtime_env: JobRuntimeEnv) -> JobHandler:
        return JobHandler(job=job, job_execution=job_runtime_env.job_execution_info)

    def stop_job(self, job_handler: JobHandler, job_runtime_env: JobRuntimeEnv):
        pass

    def cleanup_job(self, job_handler: JobHandler, job_runtime_env: JobRuntimeEnv):
        pass

    def job_type(self) -> Text:
        return "dummy"

    def get_job_generator(self) -> JobGenerator:
        return self

    def get_job_controller(self) -> JobController:
        return self

