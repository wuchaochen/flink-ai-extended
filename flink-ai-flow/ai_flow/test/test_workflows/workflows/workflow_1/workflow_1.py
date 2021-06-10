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
from ai_flow.application_master.master import AIFlowMaster
import ai_flow as af


class TestWorkflow1(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        config_file = os.path.dirname(os.path.dirname(os.path.dirname(__file__))) + '/master.yaml'
        cls.master = AIFlowMaster(config_file=config_file)
        cls.master.start()
        af.init_ai_flow_context(__file__)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.master.stop()

    def setUp(self):
        TestWorkflow1.master._clear_db()
        af.default_graph().clear_graph()

    def tearDown(self):
        TestWorkflow1.master._clear_db()

    def test_get_context(self):
        self.assertEqual('test_project', af.project_description().project_name)
        config = af.workflow_config()
        self.assertEqual('workflow_1', config.workflow_name)
        with af.job_config('task_1') as config_1:
            af.user_define_operation(executor=None)
            self.assertEqual('task_1', config_1.job_name)

    def test_define_operation(self):
        example_meta = af.register_example(name='example_1', support_type=af.ExampleSupportType.EXAMPLE_BOTH)
        with af.job_config('task_1') as config_1:
            example = af.read_example(example_info=example_meta, executor=None)
            self.assertEqual('task_1', config_1.job_name)
            node = af.default_graph().nodes.get(example.node_id)
            self.assertEqual('example_1', node.example_meta.name)


if __name__ == '__main__':
    unittest.main()
