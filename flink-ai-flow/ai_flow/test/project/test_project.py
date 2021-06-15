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
import unittest
import os
from ai_flow.common.path_util import get_file_dir
from ai_flow.project.project_description import get_project_description_from


class TestProjectConfig(unittest.TestCase):

    def test_load_project_desc(self):
        project_path = get_file_dir(__file__)
        project_desc = get_project_description_from(project_path)
        self.assertTrue(project_desc.get_absolute_project_config_file().endswith('project.yaml'))
        self.assertTrue(project_desc.get_absolute_temp_path().endswith('temp'))
        self.assertTrue(project_desc.get_absolute_workflows_path().endswith('workflows'))
        self.assertTrue(project_desc.get_absolute_resources_path().endswith('resources'))
        self.assertTrue(project_desc.get_absolute_dependencies_path().endswith('dependencies'))
        self.assertTrue(project_desc.get_absolute_log_path().endswith('logs'))
        self.assertTrue(project_desc.get_absolute_python_dependencies_path().endswith('python'))
        self.assertTrue(project_desc.get_absolute_go_dependencies_path().endswith('go'))
        self.assertTrue(project_desc.get_absolute_jar_dependencies_path().endswith('jar'))
        workflow_1_entry = project_desc.get_absolute_workflow_entry_file('workflow_1')
        self.assertTrue(os.path.exists(workflow_1_entry))
        self.assertTrue(os.path.isfile(workflow_1_entry))
        print(project_desc.list_resources_paths())
        self.assertEqual('workflow_1.workflow_1', project_desc.get_workflow_entry_module('workflow_1'))
        self.assertEqual(project_desc.project_config.get_master_uri(), "localhost:50051")
        self.assertIsNone(project_desc.project_config.get('ai_flow config', None))
        self.assertEqual(project_desc.project_config['ai_flow_home'], '/opt/ai_flow')
        self.assertEqual(project_desc.project_config['ai_flow_job_master.host'], 'localhost')
        self.assertEqual(project_desc.project_config['ai_flow_job_master.port'], 8081)
        self.assertEqual(project_desc.project_config['ai_flow_conf'], 'taskmanager.slot=2')
        self.assertEqual(2, len(project_desc.list_workflows()))


if __name__ == '__main__':
    unittest.main()
