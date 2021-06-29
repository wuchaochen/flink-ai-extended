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
from ai_flow.context.project_context import init_project_context, project_config, project_description


class TestProjectContext(unittest.TestCase):

    def test_project_context(self):
        project_path = os.path.dirname(__file__)
        init_project_context(project_path)
        self.assertEqual('test_project', project_description().project_name)
        self.assertEqual('a', project_config().get('a'))


if __name__ == '__main__':
    unittest.main()