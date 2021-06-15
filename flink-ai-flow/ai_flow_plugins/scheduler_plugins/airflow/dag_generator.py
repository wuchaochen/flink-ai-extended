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
from datetime import datetime
from typing import Text, List, Dict
from ai_flow.workflow.job import Job
from ai_flow.common import json_utils
from ai_flow.workflow.control_edge import MetConfig, AIFlowInnerEventType
from ai_flow.workflow.periodic_config import PeriodicConfig
from ai_flow.workflow.workflow import Workflow, WorkflowPropertyKeys


def import_job_plugins_text(workflow: Workflow):
    text = ''
    plugins: Dict = workflow.properties.get(WorkflowPropertyKeys.JOB_PLUGINS)
    for module, name in plugins.values():
        text += 'from {} import {}\n'.format(module, name)
    return text


class DAGTemplate(object):
    AIRFLOW_IMPORT = """
from datetime import datetime
from airflow.models.dag import DAG
from ai_flow_plugins.scheduler_plugins.airflow.event_handler import AIFlowHandler
from ai_flow_plugins.scheduler_plugins.airflow.ai_flow_operator import AIFlowOperator
from ai_flow.project.project_description import get_project_description_from
from ai_flow.common import json_utils

"""
    LOAD_CONFIG = """
project_path = '{}'
project_desc = get_project_description_from(project_path)
workflow_json = '{}'
workflow = json_utils.loads(workflow_json)
"""

    DEFAULT_ARGS = """default_args = {0}\n"""
    DEFAULT_ARGS_VALUE = {'schedule_interval': None, 'start_date': datetime.utcnow()}

    DAG_DEFINE = """dag = DAG(dag_id='{0}', default_args=default_args)\n"""

    PERIODIC_CONFIG = """op_{0}.executor_config = {{'periodic_config': {1}}}\n"""

    UPSTREAM_OP = """{0}.set_upstream({1})\n"""

    EVENT_DEPS = """{0}.subscribe_event('{1}', '{2}', '{3}', '{4}')\n"""

    MET_HANDLER = """configs_{0}='{1}'
{0}.set_events_handler(AIFlowHandler(configs_{0}))\n"""


class DAGGenerator(object):
    def __init__(self):
        self.op_count = -1

    def generate_op_code(self, job):
        self.op_count += 1
        OP_DEFINE = """
job_json_{0} = '{1}'
job_{0} = json_utils.loads(job_json_{0})
op_{0} = AIFlowOperator(task_id='{2}', job=job_{0}, project_desc=project_desc, workflow=workflow, dag=dag)
"""
        return 'job_{}'.format(self.op_count), OP_DEFINE.format(self.op_count, json_utils.dumps(job),
                                                                job.job_name)

    def generate_upstream(self, op_1, op_2):
        return DAGTemplate.UPSTREAM_OP.format(op_1, op_2)

    def generate_event_deps(self, op, from_task_id, met_config):
        if met_config.sender is not None and '' != met_config.sender:
            sender = met_config.sender
        else:
            sender = from_task_id
        return DAGTemplate.EVENT_DEPS.format(op, met_config.event_key, met_config.event_type,
                                             met_config.namespace, sender)

    def generate_handler(self, op, configs: List[MetConfig]):
        return DAGTemplate.MET_HANDLER.format(op, json_utils.dumps(configs))

    def generate(self,
                 workflow: Workflow,
                 project_name: Text,
                 project_path: Text,
                 default_args=None) -> Text:
        code_text = DAGTemplate.AIRFLOW_IMPORT
        code_text += DAGTemplate.LOAD_CONFIG.format(project_path, json_utils.dumps(workflow))
        code_text += import_job_plugins_text(workflow)

        if default_args is None:
            default_args = DAGTemplate.DEFAULT_ARGS_VALUE
        self.op_count = -1
        code_text += DAGTemplate.DEFAULT_ARGS.format(default_args)

        dag_id = '{}.{}'.format(project_name, workflow.workflow_name)
        code_text += DAGTemplate.DAG_DEFINE.format(dag_id)

        task_map = {}
        for name, job in workflow.jobs.items():
            op_name, code = self.generate_op_code(job)
            code_text += code
            task_map[job.job_name] = op_name
            # add periodic
            if name in workflow.control_edges:
                edges = workflow.control_edges.get(name)
                for edge in edges:
                    if AIFlowInnerEventType.PERIODIC_ACTION == edge.event_type:
                        periodic_config: PeriodicConfig = json_utils.loads(edge.event_value)
                        if 'interval' == periodic_config.periodic_type:
                            code_text += DAGTemplate.PERIODIC_CONFIG.format(self.op_count,
                                                                            str({'interval': periodic_config.args}))
                        elif 'cron' == periodic_config.periodic_type:
                            code_text += DAGTemplate.PERIODIC_CONFIG.format(self.op_count,
                                                                            str({'cron': periodic_config.args}))
                        else:
                            raise Exception('periodic_config do not support {} type, only support interval and cron.'
                                            .format(periodic_config.periodic_type))

        for job_name, edges in workflow.control_edges.items():
            if job_name in task_map:
                op_name = task_map[job_name]
                configs = []
                for edge in edges:
                    met_config: MetConfig = edge.generate_met_config()

                    def reset_met_config():
                        if met_config.sender is None or '' == met_config.sender:
                            target_node_id = edge.tail
                            if target_node_id is not None and '' != target_node_id:
                                target_job: Job = workflow.jobs.get(target_node_id)
                                if target_job.job_name is not None:
                                    met_config.sender = target_job.job_name
                            else:
                                met_config.sender = '*'
                    reset_met_config()

                    if edge.tail in task_map:
                        from_op_name = task_map[edge.tail]
                    else:
                        from_op_name = ''
                    code = self.generate_event_deps(op_name, from_op_name, met_config)
                    code_text += code
                    configs.append(met_config)

                if len(configs) > 0:
                    code = self.generate_handler(op_name, configs)
                    code_text += code

        return code_text
