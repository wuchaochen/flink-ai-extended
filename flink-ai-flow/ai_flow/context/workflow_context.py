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
from typing import Text

from ai_flow.context.project_context import project_description
from ai_flow.workflow.workflow_config import WorkflowConfig, load_workflow_config


class WorkflowContext(object):
    def __init__(self) -> None:
        self.workflow_config: WorkflowConfig = None


__default_workflow_context__ = WorkflowContext()


def init_workflow_context(workflow_name: Text):
    project_desc = project_description()
    __default_workflow_context__.workflow_config \
        = load_workflow_config(project_desc.get_absolute_workflow_config_file(workflow_name))


def workflow_config() -> WorkflowConfig:
    return __default_workflow_context__.workflow_config
