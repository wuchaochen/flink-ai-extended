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
from ai_flow.common.json_utils import Jsonable
from typing import Text, Dict


class PeriodicConfig(Jsonable):
    """
    Set period semantics for job. Jobs with periodic semantics will restart at regular intervals.
    """
    def __init__(self, periodic_type: Text,
                 args: Dict[Text, Jsonable] = None) -> None:
        """
        :param periodic_type: ``interval`` or ``cron``
        link apscheduler.schedulers.background.BackgroundScheduler add_job

        """
        super().__init__()
        self.periodic_type = periodic_type
        if args is None:
            args = {}
        self.args = args
