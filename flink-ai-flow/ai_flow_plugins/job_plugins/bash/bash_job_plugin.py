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
import os
import signal
from abc import ABC
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import Text, Any, Dict
from subprocess import PIPE, STDOUT, Popen
from ai_flow.common import serialization_utils
from ai_flow.workflow.job_config import JobConfig
from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import AbstractJobPlugin, BaseJobHandler
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow.project.project_description import ProjectDesc
from ai_flow.workflow.job import Job
from ai_flow_plugins.job_plugins.bash.bash_job_config import BashJobConfig
from ai_flow_plugins.job_plugins.bash.bash_executor import BashExecutor


class BashJob(Job):
    def __init__(self, job_config: JobConfig):
        super().__init__(job_config)
        self.sub_graph_path = None
        self.executors = {}


class BashJobHandler(BaseJobHandler):
    def __init__(self, job: Job,
                 job_execution: JobExecutionInfo):
        super().__init__(job=job, job_execution=job_execution)
        self.sub_process = {}


class BashJobPlugin(AbstractJobPlugin, ABC):

    def __init__(self) -> None:
        super().__init__()

    def generate(self, sub_graph: AISubGraph, project_desc: ProjectDesc) -> Job:
        bash_job_config: BashJobConfig = sub_graph.config
        job = BashJob(job_config=bash_job_config)
        for k, v in sub_graph.nodes.items():
            job.executors[k] = v.get_executor()
        return job

    def generate_job_resource(self, job: Job, project_desc: ProjectDesc) -> None:
        job_graph_path = os.path.join(project_desc.get_absolute_temp_path(), 'job_graph')
        if not os.path.exists(job_graph_path):
            os.makedirs(job_graph_path)
        with NamedTemporaryFile(mode='w+b', dir=job_graph_path, delete=False) as fp:
            job.sub_graph_path = fp.name
            fp.write(serialization_utils.serialize(job.executors))
        job.executors = None

    def submit_job(self, job: Job, project_desc: ProjectDesc, job_context: Any = None) -> BaseJobHandler:
        handler = BashJobHandler(job=job, job_execution=job_context)
        bash_job: BashJob = job
        with open(bash_job.sub_graph_path, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        for k, v in executors.items():
            executor: BashExecutor = v
            sub_process = self.submit_one_process(executor=executor,
                                                  env=job.job_config.properties.get('env'),
                                                  working_dir=project_desc.get_absolute_temp_path())
            handler.sub_process[k] = sub_process
        return handler

    def stop_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        handler: BashJobHandler = job_handler
        bash_job: BashJob = job_handler.job
        with open(bash_job.sub_graph_path, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        for k, v in executors.items():
            self.log.info('{} Output:'.format(k))
            sub_process = handler.sub_process.get(k)
            self.log.info('Sending SIGTERM signal to bash process group')
            if sub_process and hasattr(sub_process, 'pid'):
                os.killpg(os.getpgid(sub_process.pid), signal.SIGTERM)

    def cleanup_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        pass

    def wait_job_finished(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        handler: BashJobHandler = job_handler
        job: BashJob = job_handler.job
        with open(job.sub_graph_path, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        lines = {}
        for k, v in executors.items():
            self.log.info('{} Output:'.format(k))
            sub_process = handler.sub_process.get(k)
            line = ''
            for raw_line in iter(sub_process.stdout.readline, b''):
                line = raw_line.decode(v.output_encoding).rstrip()
                self.log.info("%s", line)

            sub_process.wait()

            self.log.info('Command exited with return code %s', sub_process.returncode)

            if sub_process.returncode != 0:
                raise Exception('Bash command failed. The command returned a non-zero exit code.')
            lines[k] = line
        return lines

    def job_type(self) -> Text:
        return "bash"

    def submit_one_process(self, executor: BashExecutor, env, working_dir):

        def pre_exec():
            # Restore default signal disposition and invoke setsid
            for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                if hasattr(signal, sig):
                    signal.signal(getattr(signal, sig), signal.SIG_DFL)
            os.setsid()

        self.log.info('Running command: %s', executor.bash_command)

        sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
            ['bash', "-c", executor.bash_command],
            stdout=PIPE,
            stderr=STDOUT,
            cwd=working_dir,
            env=env,
            preexec_fn=pre_exec,
        )
        return sub_process
