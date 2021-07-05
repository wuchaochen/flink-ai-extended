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
from ai_flow.ai_graph.ai_node import AINode, IONode
from ai_flow.meta.dataset_meta import DatasetMeta


class TestAINode(unittest.TestCase):

    def test_ai_node_with_var_args(self):
        node = AINode(processor=None, arg1='arg1_v', arg2='arg2_v')
        self.assertEqual('arg1_v', node.node_config['arg1'])
        self.assertEqual('arg2_v', node.node_config['arg2'])

    def test_io_node_creation(self):
        node = IONode(dataset=DatasetMeta(name='source'), is_source=True)
        self.assertEqual('source', node.node_config['dataset'].name)
        self.assertEqual(True, node.node_config['is_source'])

        node = IONode(dataset=DatasetMeta(name='sink'), is_source=False)
        self.assertEqual('sink', node.node_config['dataset'].name)
        self.assertEqual(False, node.node_config['is_source'])


if __name__ == '__main__':
    unittest.main()
