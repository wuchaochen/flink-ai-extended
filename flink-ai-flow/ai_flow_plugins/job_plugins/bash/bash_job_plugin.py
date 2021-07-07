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
import time
from tempfile import NamedTemporaryFile, mkdtemp
from typing import Text, Dict
from subprocess import PIPE, STDOUT, Popen

from ai_flow.translator.translator import JobGenerator
from ai_flow.util import serialization_utils
from ai_flow.workflow.job_config import JobConfig
from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import JobPluginFactory, JobHandle, JobRuntimeEnv, \
    JobController
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow.workflow.job import Job
from ai_flow.workflow.status import Status
from ai_flow_plugins.job_plugins.bash.bash_job_config import BashJobConfig
from ai_flow_plugins.job_plugins.bash.bash_processor import BashProcessor


class BashJob(Job):
    def __init__(self, job_config: JobConfig):
        super().__init__(job_config)
        self.sub_graph_path = None


class BashJobHandle(JobHandle):

    def __init__(self, job: Job,
                 job_execution: JobExecutionInfo):
        super().__init__(job=job, job_execution=job_execution)
        self.sub_process = {}
        self.sub_graph_path = None
        self.lines = {}

    def try_get_result(self) -> object:
        return self.lines

    def wait_until_finish(self):
        with open(self.sub_graph_path, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        for k, v in executors.items():
            self.log.info('{} Output:'.format(k))
            sub_process = self.sub_process.get(k)
            line = ''
            for raw_line in iter(sub_process.stdout.readline, b''):
                line = raw_line.decode(v.output_encoding).rstrip()
                self.log.info("%s", line)

            sub_process.wait()

            self.log.info('Command exited with return code %s', sub_process.returncode)

            if sub_process.returncode != 0:
                raise Exception('Bash command failed. The command returned a non-zero exit code {}.'
                                .format(sub_process.returncode))
            self.lines[k] = line


class BashJobPluginFactory(JobPluginFactory, JobGenerator, JobController):
    def __init__(self) -> None:
        super().__init__()

    def generate(self, sub_graph: AISubGraph, resource_dir: Text = None) -> Job:
        bash_job_config: BashJobConfig = sub_graph.config
        job = BashJob(job_config=bash_job_config)
        executors = {}
        for k, v in sub_graph.nodes.items():
            executors[k] = v.get_processor()
        with NamedTemporaryFile(mode='w+b', dir=resource_dir, prefix='{}_bash_'.format(job.job_name), delete=False) as fp:
            job.sub_graph_path = os.path.basename(fp.name)
            fp.write(serialization_utils.serialize(executors))
        return job

    def submit_job(self, job: Job, job_runtime_env: JobRuntimeEnv) -> JobHandle:
        handler = BashJobHandle(job=job, job_execution=job_runtime_env.job_execution_info)
        bash_job: BashJob = job
        executor_file = os.path.join(job_runtime_env.generated_dir, bash_job.sub_graph_path)
        with open(executor_file, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        for k, v in executors.items():
            executor: BashProcessor = v
            env = os.environ.copy()
            if 'env' in job.job_config.properties:
                env.update(job.job_config.properties.get('env'))
            sub_process = self.submit_one_process(executor=executor,
                                                  env=env,
                                                  working_dir=job_runtime_env.working_dir)
            handler.sub_process[k] = sub_process
        handler.sub_graph_path = executor_file
        return handler

    def stop_job(self, job_handle: JobHandle, job_runtime_env: JobRuntimeEnv = None):
        handler: BashJobHandle = job_handle
        executor_file = job_handle.sub_graph_path
        with open(executor_file, 'rb') as f:
            executors: Dict = serialization_utils.deserialize(f.read())
        for k, v in executors.items():
            self.log.info('{} Output:'.format(k))
            sub_process = handler.sub_process.get(k)
            self.log.info('Sending SIGTERM signal to bash process group')
            if sub_process and hasattr(sub_process, 'pid') and sub_process.poll() is None:
                while sub_process.poll() is None:
                    try:
                        os.killpg(os.getpgid(sub_process.pid), signal.SIGTERM)
                    except Exception:
                        time.sleep(1)

    def cleanup_job(self, job_handle: JobHandle, job_runtime_env: JobRuntimeEnv = None):
        pass

    def get_job_generator(self) -> JobGenerator:
        return self

    def get_job_controller(self) -> JobController:
        return self

    def job_type(self) -> Text:
        return "bash"

    def submit_one_process(self, executor: BashProcessor, env, working_dir):

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

    def get_result(self, job_handle: JobHandle, blocking: bool = True) -> object:
        handle: BashJobHandle = job_handle
        if blocking:
            with open(handle.sub_graph_path, 'rb') as f:
                executors: Dict = serialization_utils.deserialize(f.read())
            for k, v in executors.items():
                self.log.info('{} Output:'.format(k))
                sub_process = handle.sub_process.get(k)
                line = ''
                for raw_line in iter(sub_process.stdout.readline, b''):
                    line = raw_line.decode(v.output_encoding).rstrip()
                    self.log.info("%s", line)

                sub_process.wait()

                self.log.info('Command exited with return code %s', sub_process.returncode)

                if sub_process.returncode != 0:
                    raise Exception('Bash command failed. The command returned a non-zero exit code {}.'
                                    .format(sub_process.returncode))
                handle.lines[k] = line
        return handle.lines

    def get_job_status(self, job_handle: JobHandle) -> Status:
        pass
