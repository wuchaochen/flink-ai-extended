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
from typing import Text

from ai_flow.ai_graph.ai_graph import AISubGraph
from ai_flow.plugin_interface.job_plugin_interface import register_job_plugin, AbstractJobPlugin, JobHandler, \
    get_registered_job_plugins
from ai_flow.project.project_description import ProjectDesc
from ai_flow.workflow.job import Job


class MockJobPlugin(AbstractJobPlugin):
    def generate(self, sub_graph: AISubGraph, project_desc: ProjectDesc) -> Job:
        pass

    def generate_job_resource(self, job: Job, project_desc: ProjectDesc) -> None:
        pass

    def submit_job(self, job: Job, project_desc: ProjectDesc) -> JobHandler:
        pass

    def stop_job(self, job_handler: JobHandler, project_desc: ProjectDesc):
        pass

    def cleanup_job(self, job_handler: JobHandler, project_desc: ProjectDesc):
        pass

    def job_type(self) -> Text:
        return 'mock'


class TestJobPlugin(unittest.TestCase):

    def test_register_job_plugin(self):
        register_job_plugin(MockJobPlugin())
        plugins = get_registered_job_plugins()
        module, name = plugins.get('mock')
        self.assertEqual(MockJobPlugin.__name__, name)
        self.assertEqual(MockJobPlugin.__module__, module)


if __name__ == '__main__':
    unittest.main()
