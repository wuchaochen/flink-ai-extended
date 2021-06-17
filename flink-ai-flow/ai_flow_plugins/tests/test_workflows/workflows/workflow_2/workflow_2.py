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
import time
from typing import List

from notification_service.client import NotificationClient

from ai_flow_plugins.job_plugins.python.python_executor import ExecutionContext
from test_utils.base_scheduler_test import BaseSchedulerTest
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.models.taskexecution import TaskExecution
from airflow.utils.session import create_session
from ai_flow_plugins.job_plugins import python
from ai_flow_plugins.job_plugins import bash
import ai_flow as af


class MockPythonExecutor(python.PythonExecutor):

    def execute(self, execution_context: ExecutionContext, input_list: List) -> List:
        print('Hello world!')
        return []


class TestWorkflow2(BaseSchedulerTest):

    def setUp(self):
        super().setUp()
        af.init_ai_flow_context(__file__)

    def test_dummy_task(self):
        project_name = af.project_description().project_name
        workflow_name = af.workflow_config().workflow_name
        dag_id = '{}.{}'.format(project_name, workflow_name)

        def run_task_function(client: NotificationClient):
            with af.job_config('task_1'):
                af.user_define_operation(executor=None)

            workflow_info = af.workflow_operation.submit_workflow(workflow_name)
            self.assertEqual(project_name, workflow_info.namespace)
            self.assertEqual(workflow_name, workflow_info.workflow_name)

            we = af.workflow_operation.start_new_workflow_execution(workflow_name)
            while True:
                with create_session() as session:
                    dag_run = session.query(DagRun) \
                        .filter(DagRun.dag_id == '{}.{}'.format(project_name, workflow_name)).first()
                    if dag_run is not None and dag_run.state == State.SUCCESS:
                        break
                    else:
                        time.sleep(1)

        self.run_ai_flow(dag_id, run_task_function)
        with create_session() as session:
            tes = session.query(TaskExecution).filter(TaskExecution.dag_id == '{}.{}'.format(project_name, workflow_name),
                                                      TaskExecution.task_id == 'task_1').all()
            self.assertEqual(1, len(tes))

    def test_bash_task(self):
        project_name = af.project_description().project_name
        workflow_name = af.workflow_config().workflow_name
        dag_id = '{}.{}'.format(project_name, workflow_name)

        def run_task_function(client: NotificationClient):
            with af.job_config('task_2'):
                af.user_define_operation(executor=bash.BashExecutor(bash_command='echo "hello world!"'))
                af.user_define_operation(executor=bash.BashExecutor(bash_command='echo $a'))

            workflow_info = af.workflow_operation.submit_workflow(workflow_name)
            self.assertEqual(project_name, workflow_info.namespace)
            self.assertEqual(workflow_name, workflow_info.workflow_name)

            we = af.workflow_operation.start_new_workflow_execution(workflow_name)
            while True:
                with create_session() as session:
                    dag_run = session.query(DagRun) \
                        .filter(DagRun.dag_id == '{}.{}'.format(project_name, workflow_name)).first()
                    if dag_run is not None and dag_run.state == State.SUCCESS:
                        break
                    else:
                        time.sleep(1)

        self.run_ai_flow(dag_id, run_task_function)
        with create_session() as session:
            tes = session.query(TaskExecution).filter(TaskExecution.dag_id == '{}.{}'.format(project_name, workflow_name),
                                                      TaskExecution.task_id == 'task_2').all()
            self.assertEqual(1, len(tes))

    def test_python_task(self):
        project_name = af.project_description().project_name
        workflow_name = af.workflow_config().workflow_name
        dag_id = '{}.{}'.format(project_name, workflow_name)

        def run_task_function(client: NotificationClient):
            with af.job_config('task_3'):
                af.user_define_operation(executor=MockPythonExecutor())

            workflow_info = af.workflow_operation.submit_workflow(workflow_name)
            self.assertEqual(project_name, workflow_info.namespace)
            self.assertEqual(workflow_name, workflow_info.workflow_name)

            we = af.workflow_operation.start_new_workflow_execution(workflow_name)
            while True:
                with create_session() as session:
                    dag_run = session.query(DagRun) \
                        .filter(DagRun.dag_id == '{}.{}'.format(project_name, workflow_name)).first()
                    if dag_run is not None and dag_run.state == State.SUCCESS:
                        break
                    else:
                        time.sleep(1)

        self.run_ai_flow(dag_id, run_task_function)
        with create_session() as session:
            tes = session.query(TaskExecution).filter(TaskExecution.dag_id == '{}.{}'.format(project_name, workflow_name),
                                                      TaskExecution.task_id == 'task_3').all()
            self.assertEqual(1, len(tes))


if __name__ == '__main__':
    unittest.main()
