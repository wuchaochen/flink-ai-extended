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
import os
from ai_flow import AIFlowServerRunner, init_ai_flow_context, PeriodicConfig
import ai_flow as af
from ai_flow.workflow.control_edge import TaskAction
from ai_flow.workflow.status import Status
from ai_flow.test.api.mock_plugins import MockJobFactory


class TestAirFlowScheduler(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config_file = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + '/master.yaml'
        cls.master = AIFlowServerRunner(config_file=config_file)
        cls.master.start()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.master.stop()

    def setUp(self):
        self.master._clear_db()
        init_ai_flow_context()

    def tearDown(self):
        self.master._clear_db()

    def test_pause_workflow(self):
        workflow_info = af.workflow_operation.pause_workflow_scheduling(
            workflow_name=af.current_workflow_config().workflow_name)
        self.assertEqual(af.current_workflow_config().workflow_name, workflow_info.workflow_name)

    def test_resume_workflow(self):
        workflow_info = af.workflow_operation.resume_workflow_scheduling(
            workflow_name=af.current_workflow_config().workflow_name)
        self.assertEqual(af.current_workflow_config().workflow_name, workflow_info.workflow_name)

    def test_get_workflow_execution(self):
        workflow_execution_info = af.workflow_operation.get_workflow_execution(execution_id='1')
        self.assertEqual('1', workflow_execution_info.workflow_execution_id)

    def test_list_workflow_executions(self):
        workflow_execution_info_list = af.workflow_operation.list_workflow_executions(
            workflow_name=af.current_workflow_config().workflow_name)
        self.assertEqual('1', workflow_execution_info_list[0].workflow_execution_id)

    def test_get_job_execution(self):
        job_execution_info = af.workflow_operation.get_job_execution(execution_id='1', job_name='job_name')
        self.assertEqual('job_name', job_execution_info.job_name)

    def test_list_job_executions(self):
        job_execution_info_list = af.workflow_operation.list_job_executions(execution_id='1')
        self.assertEqual('job_name', job_execution_info_list[0].job_name)


if __name__ == '__main__':
    unittest.main()
