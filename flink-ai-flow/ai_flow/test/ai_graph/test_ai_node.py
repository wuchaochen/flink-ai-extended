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
from ai_flow.ai_graph.ai_node import AINode


class TestCustomAINode(unittest.TestCase):

    def test_ai_node(self):
        node = AINode(executor=None, arg1='arg1_v', arg2='arg2_v')
        self.assertEqual('arg1_v', node.node_config['arg1'])
        self.assertEqual('arg2_v', node.node_config['arg2'])


if __name__ == '__main__':
    unittest.main()
