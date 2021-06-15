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
from typing import Dict, Text
import copy
from ai_flow.translator.base_translator import BaseGraphSplitter, BaseJobGenerator, BaseWorkflowConstructor, \
    BaseTranslator
from ai_flow.rest_endpoint.service.client.aiflow_client import AIFlowClient
from ai_flow.workflow.workflow import Workflow
from ai_flow.workflow.job import Job
from ai_flow.ai_graph.ai_graph import AIGraph, SplitGraph, AISubGraph
from ai_flow.ai_graph.data_edge import DataEdge
from ai_flow.workflow.control_edge import ControlEdge
from ai_flow.project.project_description import ProjectDesc


class GraphSplitter(BaseGraphSplitter):

    def __init__(self) -> None:
        super().__init__()

    def split(self, graph: AIGraph, project_desc: ProjectDesc) -> SplitGraph:

        split_graph = SplitGraph()

        for n in graph.nodes.values():
            job_name = n.config.job_name
            if job_name in split_graph.nodes:
                sub_graph = split_graph.nodes.get(job_name)
            else:
                sub_graph = AISubGraph(config=n.config)
                split_graph.add_node(sub_graph)
            sub_graph.add_node(n)

        # add data edge to sub graph
        for sub_graph in split_graph.nodes.values():
            for n in sub_graph.nodes.values():
                if n.instance_id in graph.edges:
                    for e in graph.edges[n.instance_id]:
                        if isinstance(e, DataEdge):
                            sub_graph.add_edge(n.instance_id, e)

        for e in graph.edges:
            for ee in graph.edges[e]:
                if isinstance(ee, ControlEdge):
                    split_graph.add_edge(ee.head, ee)
        return split_graph


class WorkflowConstructor(BaseWorkflowConstructor):
    class JobGeneratorRegistry(object):
        def __init__(self) -> None:
            super().__init__()
            self.object_dict: Dict[Text, BaseJobGenerator] = {}

        def register(self, key: Text, value: BaseJobGenerator):
            self.object_dict[key] = value

        def get_object(self, key: Text) -> BaseJobGenerator:
            return self.object_dict[key]

    def __init__(self) -> None:
        super().__init__()
        self.job_generator_registry: WorkflowConstructor.JobGeneratorRegistry \
            = WorkflowConstructor.JobGeneratorRegistry()
        self.client: AIFlowClient = None

    def register_job_generator(self, engine, generator: BaseJobGenerator):
        self.job_generator_registry.register(engine, generator)

    def build_workflow(self, split_graph: SplitGraph, project_desc: ProjectDesc) -> Workflow:
        workflow = Workflow()
        # add ai_nodes to workflow
        for sub in split_graph.nodes.values():
            if sub.config.job_type not in self.job_generator_registry.object_dict:
                raise Exception("job generator not support job_type {}"
                                .format(sub.config.job_type))
            generator: BaseJobGenerator = self.job_generator_registry \
                .get_object(sub.config.job_type)
            job: Job = generator.generate(sub_graph=sub, project_desc=project_desc)
            workflow.add_job(job)

        def validate_edge(head, tail):
            if head not in workflow.jobs:
                raise Exception('job: {} is not defined in workflow!'.format(head))
            if tail is not None and tail != '' and tail != '*' and tail not in workflow.jobs:
                raise Exception('job: {} is not defined in workflow!'.format(tail))
        # add edges to workflow
        for edges in split_graph.edges.values():
            for e in edges:
                control_edge = copy.deepcopy(e)
                validate_edge(control_edge.head, control_edge.tail)
                workflow.add_edge(control_edge.head, control_edge)

        for job in workflow.nodes.values():
            generator: BaseJobGenerator = self.job_generator_registry \
                .get_object(job.job_config.job_type)
            generator.generate_job_resource(job, project_desc)
        return workflow


class Translator(BaseTranslator):
    def __init__(self,
                 graph_splitter: GraphSplitter,
                 workflow_constructor: WorkflowConstructor
                 ) -> None:
        super().__init__()
        self.graph_splitter = graph_splitter
        self.workflow_constructor = workflow_constructor

    def translate(self, graph: AIGraph, project_desc: ProjectDesc) -> Workflow:
        split_graph = self.graph_splitter.split(graph=graph,
                                                project_desc=project_desc)
        workflow = self.workflow_constructor.build_workflow(split_graph=split_graph,
                                                            project_desc=project_desc)
        return workflow


__default_translator__ = Translator(graph_splitter=GraphSplitter(),
                                    workflow_constructor=WorkflowConstructor())


def get_default_translator() -> BaseTranslator:
    return __default_translator__


def register_job_generator(job_type, generator: BaseJobGenerator) -> None:
    __default_translator__.workflow_constructor.register_job_generator(job_type, generator)
