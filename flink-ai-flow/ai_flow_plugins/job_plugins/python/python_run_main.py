#
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
#
import importlib
import sys
from ai_flow.common.serialization_utils import deserialize
from ai_flow.api.ai_flow_context import init_ai_flow_context_by_name
from ai_flow.plugin_interface.scheduler_interface import JobExecutionInfo
from ai_flow_plugins.job_plugins.python.python_job_plugin import python_execute_func, RunGraph, RunArgs


def read_from_serialized_file(file_path):
    with open(file_path, 'rb') as f:
        serialized_bytes = f.read()
    return deserialize(serialized_bytes)


def run_project(run_graph_file, run_args_file):
    run_graph: RunGraph = read_from_serialized_file(run_graph_file)
    args: RunArgs = read_from_serialized_file(run_args_file)
    project_path = args.project_path
    job_execution_info: JobExecutionInfo = args.job_execution_info

    workflow_name = job_execution_info.workflow_execution.workflow_info.workflow_name
    entry_module_path = workflow_name
    init_ai_flow_context_by_name(project_path=project_path, workflow_name=workflow_name)
    mdl = importlib.import_module(entry_module_path)
    if "__all__" in mdl.__dict__:
        names = mdl.__dict__["__all__"]
    else:
        names = [x for x in mdl.__dict__ if not x.startswith("_")]
    globals().update({k: getattr(mdl, k) for k in names})
    try:
        python_execute_func(run_graph=run_graph, job_execution_info=job_execution_info)
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise Exception(str(e))


if __name__ == '__main__':
    l_graph_file, l_args_file = sys.argv[1], sys.argv[2]
    run_project(l_graph_file, l_args_file)
