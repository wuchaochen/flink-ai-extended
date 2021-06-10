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
import os
from typing import Dict, Text
from ai_flow.common.json_utils import Jsonable, loads
from ai_flow.workflow.job_config import JobConfig
from ai_flow.common import yaml_utils

WORKFLOW_PROPERTIES = "properties"
WORKFLOW_DEPENDENCIES = "dependencies"


class WorkFlowConfig(Jsonable):

    def __init__(self, workflow_name: Text = None) -> None:
        super().__init__()
        self.workflow_name = workflow_name
        self.job_configs: Dict[Text, JobConfig] = {}
        self.properties: Dict[Text, Jsonable] = {}
        self.dependencies: Dict = None

    def add_job_config(self, config_key: Text, job_config: JobConfig):
        self.job_configs[config_key] = job_config


def load_workflow_config(config_path: Text) -> WorkFlowConfig:
    if config_path.endswith('.json'):
        with open(config_path, 'r') as f:
            workflow_config_json = f.read()
        workflow_config: WorkFlowConfig = loads(workflow_config_json)
        return workflow_config
    elif config_path.endswith('.yaml'):
        workflow_name = os.path.basename(config_path)[:-5]
        workflow_data = yaml_utils.load_yaml_file(config_path)

        workflow_config: WorkFlowConfig = WorkFlowConfig(workflow_name=workflow_name)

        if WORKFLOW_PROPERTIES in workflow_data:
            workflow_config.properties = workflow_data[WORKFLOW_PROPERTIES]

        if WORKFLOW_DEPENDENCIES in workflow_data:
            workflow_config.dependencies = workflow_data[WORKFLOW_DEPENDENCIES]

        for k, v in workflow_data.items():
            if k == WORKFLOW_DEPENDENCIES or k == WORKFLOW_PROPERTIES:
                continue
            job_config = JobConfig.from_dict(k, v)
            workflow_config.add_job_config(k, job_config)
        return workflow_config
    else:
        return None
