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
import sys
import signal
import logging
from abc import ABC
from tempfile import NamedTemporaryFile
from typing import Text, Any, Dict, List
from subprocess import Popen

from ai_flow.log import log_path_utils
from ai_flow.ai_graph.ai_node import AINode
from ai_flow.ai_graph.data_edge import DataEdge
from ai_flow.common import serialization_utils
from ai_flow.common import json_utils
from ai_flow.workflow.job_config import JobConfig
from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import AbstractJobPlugin, BaseJobHandler
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow.project.project_description import ProjectDesc
from ai_flow.workflow.job import Job
from ai_flow_plugins.job_plugins.flink.flink_job_config import FlinkJobConfig
from ai_flow_plugins.job_plugins.flink.flink_executor import FlinkPythonExecutor, FlinkJavaExecutor, ExecutionContext
from ai_flow_plugins.job_plugins.flink.flink_env import get_flink_env, AbstractFlinkEnv
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, BatchTableEnvironment, StreamTableEnvironment, TableEnvironment, StatementSet


class RunGraph(json_utils.Jsonable):
    def __init__(self) -> None:
        super().__init__()
        self.nodes: List[AINode] = []
        self.executor_bytes: List[bytes] = []
        self.dependencies: Dict[Text, List[DataEdge]] = {}


class RunArgs(json_utils.Jsonable):
    def __init__(self,
                 project_path: Text,
                 entry_module_path: Text,
                 job_execution_info: JobExecutionInfo) -> None:
        super().__init__()
        self.project_path: Text = project_path
        self.entry_module_path: Text = entry_module_path
        self.job_execution_info: JobExecutionInfo = job_execution_info


def flink_execute_func(run_graph: RunGraph, job_execution_info: JobExecutionInfo, flink_env: AbstractFlinkEnv):
    executors: List[FlinkPythonExecutor] = []
    contexts: List[ExecutionContext] = []
    exec_env, table_env, statement_set = flink_env.create_env()
    for index in range(len(run_graph.nodes)):
        caller: FlinkPythonExecutor = serialization_utils.deserialize(run_graph.executor_bytes[index])
        executors.append(caller)
        node: AINode = run_graph.nodes[index]
        execution_context = ExecutionContext(node_spec=node,
                                             job_execution_info=job_execution_info,
                                             execution_env=exec_env,
                                             table_env=table_env,
                                             statement_set=statement_set)
        contexts.append(execution_context)

    def setup():
        for ii in range(len(executors)):
            cc = executors[ii]
            cc.setup(contexts[ii])

    def close():
        for ii in range(len(executors)):
            cc = executors[ii]
            cc.close(contexts[ii])

    setup()
    value_map = {}
    for i in range(len(run_graph.nodes)):
        node = run_graph.nodes[i]
        c = executors[i]
        if node.instance_id in run_graph.dependencies:
            ds = run_graph.dependencies[node.instance_id]
            params = []
            for d in ds:
                params.append(value_map[d.tail][d.port])
            value_map[node.instance_id] = c.execute(contexts[i], params)
        else:
            value_map[node.instance_id] = c.execute(contexts[i], [])
    close()
    job_client = statement_set.execute().get_job_client()
    if job_client is not None:
        job_client.get_job_execution_result(user_class_loader=None).result()


class FlinkJob(Job):
    def __init__(self, job_config: JobConfig, run_graph: RunGraph):
        super().__init__(job_config)
        self.run_graph = run_graph
        self.run_graph_file: Text = None
        self.flink_env_file: Text = None


class FlinkJobHandler(BaseJobHandler):
    def __init__(self, job: Job,
                 job_execution: JobExecutionInfo):
        super().__init__(job=job, job_execution=job_execution)
        self.sub_process = None
        self.run_args_file = None


class FlinkJobPlugin(AbstractJobPlugin, ABC):

    def __init__(self) -> None:
        super().__init__()

    @classmethod
    def build_run_graph(cls, sub_graph: AISubGraph) -> RunGraph:
        run_graph = RunGraph()
        processed_nodes = set()
        node_list: List[AINode] = []
        for n in sub_graph.nodes.values():
            node_list.append(n)
        for e in sub_graph.edges:
            data_channel_list = []
            for c in sub_graph.edges[e]:
                cc: DataEdge = c
                data_channel_list.append(cc)
            run_graph.dependencies[e] = data_channel_list

        node_size = len(sub_graph.nodes)
        processed_size = len(processed_nodes)
        while processed_size != node_size:
            p_nodes = []
            for i in range(len(node_list)):
                if node_list[i].instance_id in sub_graph.edges:
                    flag = True
                    for c in sub_graph.edges[node_list[i].instance_id]:
                        if c.tail in processed_nodes:
                            pass
                        else:
                            flag = False
                            break
                else:
                    flag = True
                if flag:
                    p_nodes.append(node_list[i])
            if 0 == len(p_nodes):
                raise Exception("graph has circle!")
            for n in p_nodes:
                run_graph.nodes.append(n)
                run_graph.executor_bytes.append(n.executor)
                node_list.remove(n)
                processed_nodes.add(n.instance_id)
            processed_size = len(processed_nodes)
        return run_graph

    def generate(self, sub_graph: AISubGraph, project_desc: ProjectDesc) -> Job:
        flink_job_config: FlinkJobConfig = sub_graph.config
        run_graph: RunGraph = self.build_run_graph(sub_graph)
        return FlinkJob(job_config=flink_job_config, run_graph=run_graph)

    def generate_job_resource(self, job: Job, project_desc: ProjectDesc) -> None:
        flink_job: FlinkJob = job
        job_graph_path = os.path.join(project_desc.get_absolute_temp_path(), 'flink')
        if not os.path.exists(job_graph_path):
            os.makedirs(job_graph_path)
        with NamedTemporaryFile(mode='w+b', dir=job_graph_path,
                                prefix='{}_run_graph'.format(job.job_name), delete=False) as fp:
            flink_job.run_graph_file = fp.name
            fp.write(serialization_utils.serialize(flink_job.run_graph))
        flink_job.run_graph = None

        with NamedTemporaryFile(mode='w+b', dir=job_graph_path,
                                prefix='{}_flink_env'.format(job.job_name), delete=False) as fp:
            flink_job.flink_env_file = fp.name
            fp.write(serialization_utils.serialize(get_flink_env()))

    def submit_job(self, job: Job, project_desc: ProjectDesc, job_context: Any = None) -> BaseJobHandler:
        job_execution_info: JobExecutionInfo = job_context
        run_args: RunArgs = RunArgs(project_path=project_desc.project_path,
                                    entry_module_path=job_execution_info.workflow_execution.workflow_info.workflow_name,
                                    job_execution_info=job_execution_info)

        job_graph_path = os.path.join(project_desc.get_absolute_temp_path(), 'flink')
        with NamedTemporaryFile(mode='w+b', dir=job_graph_path,
                                prefix='{}_run_args'.format(job.job_name), delete=False) as fp:
            run_args_file = fp.name
            fp.write(serialization_utils.serialize(run_args))
        handler = FlinkJobHandler(job=job, job_execution=job_context)
        flink_job: FlinkJob = job
        run_graph_file = flink_job.run_graph_file
        flink_env_file = flink_job.flink_env_file
        env = os.environ.copy()
        env.update(job.job_config.properties.get('env', {}))
        # Add PYTHONEPATH
        copy_path = sys.path.copy()
        copy_path.insert(0, project_desc.get_absolute_python_dependencies_path())
        env['PYTHONPATH'] = ':'.join(copy_path)

        current_path = os.path.dirname(__file__)
        script_path = os.path.join(current_path, 'flink_run_main.py')
        python3_location = sys.executable
        bash_command = [python3_location, script_path, run_graph_file, run_args_file, flink_env_file]

        stdout_log = log_path_utils.stdout_log_path(project_desc.get_absolute_log_path(), job.job_name)
        stderr_log = log_path_utils.stderr_log_path(project_desc.get_absolute_log_path(), job.job_name)
        if not os.path.exists(project_desc.get_absolute_log_path()):
            os.mkdir(project_desc.get_absolute_log_path())

        sub_process = self.submit_process(bash_command=bash_command,
                                          env=env,
                                          working_dir=project_desc.get_absolute_temp_path(),
                                          stdout_log=stdout_log,
                                          stderr_log=stderr_log)
        handler.sub_process = sub_process
        handler.run_args_file = run_args_file
        return handler

    def stop_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        handler: FlinkJobHandler = job_handler
        self.log.info('Output:')
        sub_process = handler.sub_process
        self.log.info('Sending SIGTERM signal to python process group')
        if sub_process and hasattr(sub_process, 'pid'):
            os.killpg(os.getpgid(sub_process.pid), signal.SIGTERM)

    def cleanup_job(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        if os.path.exists(job_handler.run_args_file):
            os.remove(job_handler.run_args_file)

    def wait_job_finished(self, job_handler: BaseJobHandler, project_desc: ProjectDesc, job_context: Any = None):
        handler: FlinkJobHandler = job_handler
        self.log.info('Output:')
        sub_process = handler.sub_process

        sub_process.wait()

        self.log.info('Command exited with return code %s', sub_process.returncode)

        if sub_process.returncode != 0:
            raise Exception('Bash command failed. The command returned a non-zero exit code.')
        return None

    def job_type(self) -> Text:
        return "flink"

    def submit_process(self, bash_command: List, env, working_dir, stdout_log, stderr_log):

        def pre_exec():
            # Restore default signal disposition and invoke setsid
            for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                if hasattr(signal, sig):
                    signal.signal(getattr(signal, sig), signal.SIG_DFL)
            os.setsid()

        self.log.info('Running command: %s', bash_command)
        with open(stdout_log, 'a') as out, open(stderr_log, 'a') as err:
            sub_process = Popen(  # pylint: disable=subprocess-popen-preexec-fn
                bash_command,
                stdout=out,
                stderr=err,
                cwd=working_dir,
                env=env,
                preexec_fn=pre_exec,
            )
        return sub_process
