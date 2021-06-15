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
import unittest

from ai_flow.workflow.periodic_config import PeriodicConfig
from ai_flow.workflow.control_edge import TaskAction
from ai_flow.workflow.job_state import JobState
from ai_flow_plugins.scheduler_plugins.airflow.dag_generator import DAGGenerator
from test_utils.base_workflow_test import BaseWorkflowTest
from ai_flow.translator.translator import get_default_translator
from ai_flow_plugins.job_plugins import dummy
import ai_flow as af


class TestWorkflow1(BaseWorkflowTest):

    def setUp(self):
        super().setUp()
        af.init_ai_flow_context(__file__)

    def test_airflow_dag_generator_one_task(self):
        config = af.workflow_config()
        with af.job_config('task_1'):
            af.user_define_operation(executor=None)
        tl = get_default_translator()
        workflow = tl.translate(graph=af.default_graph(), project_desc=af.project_description())
        af.workflow_operation.apply_full_infor_to_workflow(
            af.project_description().get_workflow_entry_module(config.workflow_name),
            workflow
        )
        generator = DAGGenerator()
        code = generator.generate(workflow=workflow,
                                  project_path=af.project_description().project_path,
                                  project_name=af.project_description().project_name)
        self.assertTrue('op_0 = AIFlowOperator' in code)
        self.assertEqual(1, len(workflow.jobs))

    def test_airflow_dag_generator_two_task(self):
        config = af.workflow_config()
        with af.job_config('task_1'):
            af.user_define_operation(executor=None)
        with af.job_config('task_2'):
            af.user_define_operation(executor=None)
        af.action_on_event(job_name='task_2', event_key='a', event_type='a', event_value='a', sender='task_1')
        tl = get_default_translator()
        workflow = tl.translate(graph=af.default_graph(), project_desc=af.project_description())
        af.workflow_operation.apply_full_infor_to_workflow(
            af.project_description().get_workflow_entry_module(config.workflow_name),
            workflow
        )
        generator = DAGGenerator()
        code = generator.generate(workflow=workflow,
                                  project_path=af.project_description().project_path,
                                  project_name=af.project_description().project_name)
        self.assertTrue("job_1.subscribe_event('a', 'a', 'default', 'task_1')" in code)
        self.assertTrue("job_1.set_events_handler(AIFlowHandler(configs_job_1))" in code)
        self.assertEqual(2, len(workflow.jobs))

    def test_airflow_dag_generator_three_task(self):
        config = af.workflow_config()
        with af.job_config('task_1'):
            af.user_define_operation(executor=None)
        with af.job_config('task_2'):
            af.user_define_operation(executor=None)
        with af.job_config('task_3'):
            af.user_define_operation(executor=None)
        af.action_on_event(job_name='task_3', event_key='a', event_type='a', event_value='a', sender='task_1')
        af.action_on_status(job_name='task_3', upstream_job_name='task_2',
                            upstream_job_status=JobState.SUCCESS,
                            action=TaskAction.START)
        tl = get_default_translator()
        workflow = tl.translate(graph=af.default_graph(), project_desc=af.project_description())
        af.workflow_operation.apply_full_infor_to_workflow(
            af.project_description().get_workflow_entry_module(config.workflow_name),
            workflow
        )
        generator = DAGGenerator()
        code = generator.generate(workflow=workflow,
                                  project_path=af.project_description().project_path,
                                  project_name=af.project_description().project_name)
        self.assertTrue("job_1.subscribe_event('a', 'a', 'default', 'task_1')" in code)
        self.assertTrue("job_2.subscribe_event('workflow_1', 'JOB_STATUS_CHANGED', 'test_project', 'task_2')" in code)
        self.assertTrue("job_1.set_events_handler(AIFlowHandler(configs_job_1))" in code)
        self.assertEqual(3, len(workflow.jobs))

    def test_airflow_dag_generator_periodic_task(self):
        config = af.workflow_config()
        with af.job_config('task_1'):
            af.user_define_operation(executor=None)

        af.action_with_periodic('task_1', periodic_config=PeriodicConfig(periodic_type='cron', args={}))

        tl = get_default_translator()
        workflow = tl.translate(graph=af.default_graph(), project_desc=af.project_description())
        af.workflow_operation.apply_full_infor_to_workflow(
            af.project_description().get_workflow_entry_module(config.workflow_name),
            workflow
        )
        generator = DAGGenerator()
        code = generator.generate(workflow=workflow,
                                  project_path=af.project_description().project_path,
                                  project_name=af.project_description().project_name)
        self.assertTrue('op_0 = AIFlowOperator' in code)
        self.assertTrue('op_0.executor_config' in code)

        self.assertEqual(1, len(workflow.jobs))


if __name__ == '__main__':
    unittest.main()
