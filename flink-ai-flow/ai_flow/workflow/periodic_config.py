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
from ai_flow.util.json_utils import Jsonable
from typing import Text, Dict, List
from datetime import datetime, timedelta
from pytz import timezone


class PeriodicConfig(Jsonable):
    """
    Set period semantics for job. Jobs with periodic semantics will restart at regular intervals.
    """

    def __init__(self,
                 start_date_expression: Text = None,
                 cron_expression: Text = None,
                 interval_expression: Text = None) -> None:
        """
        :param start_date_expression:
        year:int,month:int,day:int,hour:int,minute:int,second:int,Option[tzinfo: str]
        :param cron_expression:
        seconds minutes hours days months weeks years
        :param interval_expression:
        days:float,hours:float,minutes:float,seconds:float
        """
        super().__init__()
        self.start_date_expression = start_date_expression
        self.cron_expression = cron_expression
        self.interval_expression = interval_expression

    @classmethod
    def to_dict(cls, config: 'PeriodicConfig') -> Dict:
        return {'start_date': config.start_date_expression,
                'cron': config.cron_expression,
                'interval': config.interval_expression}

    @classmethod
    def from_dict(cls, data: Dict) -> 'PeriodicConfig':
        return PeriodicConfig(start_date_expression=data['start_date'],
                              cron_expression=data['cron'],
                              interval_expression=data['interval'])

    def get_cron_items(self):
        cron_list = self.cron_expression.split(' ')
        if len(cron_list) != 7:
            raise Exception('cron expression {} is not validated! '
                            'Usage: seconds minutes hours days months weeks years')
        result = []
        for i in cron_list:
            result.append(i.strip())
        return result

    def get_start_date_items(self):
        start_date_list = self.start_date_expression.split(',')
        if len(start_date_list) == 7:
            result = []
            for i in range(len(start_date_list)):
                if i < 6:
                    if len(start_date_list[i].strip()) == 0:
                        if i < 3:
                            raise Exception('year month, day mast set!')
                        else:
                            result.append(0)
                    else:
                        result.append(int(start_date_list[i].strip()))
                else:
                    if len(start_date_list[i].strip()) == 0:
                        result.append(None)
                    else:
                        result.append(start_date_list[i].strip())
            return result
        elif len(start_date_list) == 6:
            result = []
            for i in start_date_list:
                result.append(int(i.strip()))
            return result
        else:
            raise Exception('start expression {} is not validated! '
                            'Usage: year:int,month:int,day:int,hour:int,minute:int,second:int,Option[tzinfo: str]')

    def get_interval_items(self)->List[float]:
        interval_list = self.interval_expression.split(',')
        if len(interval_list) != 4:
            raise Exception('interval expression {} is not validated! '
                            'Usage: days:float,hours:float,minutes:float,seconds:float')
        result = []
        for i in interval_list:
            if len(i.strip()) == 0:
                result.append(0.0)
            else:
                result.append(float(i.strip()))
        return result

    def get_start_date(self) -> datetime:
        tmp = self.get_start_date_items()
        if tmp[6] is None:
            return datetime(year=tmp[0],
                            month=tmp[1],
                            day=tmp[2],
                            hour=tmp[3],
                            minute=tmp[4],
                            second=tmp[5])
        else:
            return datetime(year=tmp[0],
                            month=tmp[1],
                            day=tmp[2],
                            hour=tmp[3],
                            minute=tmp[4],
                            second=tmp[5],
                            tzinfo=timezone(tmp[6]))

    def get_interval(self) -> timedelta:
        tmp = self.get_interval_items()
        return timedelta(days=tmp[0],
                         hours=tmp[1],
                         minutes=tmp[2],
                         seconds=tmp[3])