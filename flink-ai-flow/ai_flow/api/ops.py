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
from typing import Union, Text, Tuple, Optional, List

from notification_service.base_notification import UNDEFINED_EVENT_TYPE
from ai_flow.client.ai_flow_client import get_ai_flow_client
from ai_flow.ai_graph.ai_node import CustomAINode
from ai_flow.common.args import ExecuteArgs
from ai_flow.graph.channel import Channel, NoneChannel
from ai_flow.graph.edge import StopBeforeControlEdge, \
    ModelVersionControlEdge, ExampleControlEdge, UserDefineControlEdge, \
    TaskAction, EventLife, MetValueCondition, MetCondition, DEFAULT_NAMESPACE
from ai_flow.ai_graph.ai_graph import default_graph, add_ai_node_to_graph
from ai_flow.meta.example_meta import ExampleMeta
from ai_flow.meta.model_meta import ModelMeta, ModelVersionMeta


def read_example(example_info: Union[ExampleMeta, Text, int],
                 executor=None,
                 exec_args: Optional[ExecuteArgs] = None) -> Channel:
    """
    Read example from the example operator. It can read example from external system.

    :param example_info: Information about the example which will be read. Its type can be ExampleMeta
                         of py:class:`ai_flow.meta.example_meta.ExampleMeta` or Text or int. The example_info
                         means name in the metadata service when its type is Text and it means id when its type is int.
                         The ai flow will get the example from metadata service by name or id.
    :param executor: The python user defined function in read example operator. User can write their own logic here.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :return: Channel: data output channel.
    """
    if isinstance(example_info, ExampleMeta):
        example_meta = example_info
    elif isinstance(example_info, Text):
        example_meta = get_ai_flow_client().get_example_by_name(example_info)
    else:
        example_meta = get_ai_flow_client().get_example_by_id(example_info)

    return user_define_operation(example_meta=example_meta,
                                 properties=exec_args,
                                 is_source=True,
                                 executor=executor,
                                 output_num=1)


def write_example(input_data: Channel,
                  example_info: Union[ExampleMeta, Text, int],
                  executor=None,
                  exec_args: ExecuteArgs = None
                  ) -> NoneChannel:
    """
    Write example to example operator. It can write example to external system.

    :param input_data: Channel from the specific operator which generates data.
    :param example_info: Information about the example which will be read. Its type can be ExampleMeta
                         of py:class:`ai_flow.meta.example_meta.ExampleMeta` or Text or int. The example_info
                         means name in he metadata service when its type is Text and it means id when its type is int.
                         The ai flow will get the example from metadata service by name or id.
    :param executor: The python user defined function in write example operator. User can write their own logic here.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :return: NoneChannel.
    """
    if isinstance(example_info, ExampleMeta):
        example_meta = example_info
    elif isinstance(example_info, Text):
        example_meta = get_ai_flow_client().get_example_by_name(example_info)
    else:
        example_meta = get_ai_flow_client().get_example_by_id(example_info)

    return user_define_operation(input_data=input_data,
                                 example_meta=example_meta,
                                 properties=exec_args,
                                 is_source=False,
                                 executor=executor,
                                 output_num=0)


def transform(input_data_list: List[Channel],
              executor,
              exec_args: ExecuteArgs = None,
              output_num=1,
              name: Text = None) -> Union[Channel, Tuple[Channel]]:
    """
    Transformer operator. Transform the example so that the original example can be used for trainer or other operators
    after feature engineering, data cleaning or some other data transformation.

    :param input_data_list: List of input data. It contains multiple channels from the operators which generate data.
    :param executor: The user defined function in transform operator. User can write their own logic here.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param output_num: The output number of the operator. The default value is 1.
    :param name: Name of the transform operator.
    :return: Channel or Tuple[Channel]. It returns Channel When the output_num is 1 and returns Tuple[Channel] when
             the output_num is bigger than 1, and the len(Tuple(Channel)) is output_num.
    """
    return user_define_operation(input_data_list=input_data_list,
                                 name=name,
                                 executor=executor,
                                 properties=exec_args,
                                 output_num=output_num)


def train(input_data_list: List[Channel],
          executor,
          model_info: Union[ModelMeta, Text, int],
          base_model_info: Union[ModelMeta, Text, int] = None,
          exec_args: ExecuteArgs = None,
          output_num=0,
          name: Text = None) -> Union[NoneChannel, Channel, Tuple[Channel]]:
    """
    Trainer operator. Train model with the inputs or continually re-training the base model.

    :param input_data_list: List of Channel. It contains multiple channels from the operators which generate data.
    :param executor: The user defined function in train operator. User can write their own logic here.
    :param model_info: Information about the output model which is under training. Its type can be ModelMeta
                              of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The output_model_info
                              means name in he metadata service when its type is Text and it means id when its type is
                              int. The ai flow will get the model meta from metadata service by name or id.
    :param base_model_info: Information about the base model which will be trained. Its type can be ModelMeta
                            of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The base_model_info
                            means name in he metadata service when its type is Text and it means id when its type is
                            int. The ai flow will get the model meta from metadata service by name or id.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param output_num: The output number of the operator. The default value is 0.
    :param name: Name of the train operator.
    :return: NoneChannel, Channel, Tuple[Channel].
    """
    if isinstance(model_info, ModelMeta):
        output_model_meta = model_info
    elif isinstance(model_info, Text):
        output_model_meta = get_ai_flow_client().get_model_by_name(model_info)
    else:
        output_model_meta = get_ai_flow_client().get_model_by_id(model_info)

    if base_model_info is not None:
        if isinstance(base_model_info, ModelMeta):
            base_model_meta = base_model_info
        elif isinstance(base_model_info, Text):
            base_model_meta = get_ai_flow_client().get_model_by_name(base_model_info)
        else:
            base_model_meta = get_ai_flow_client().get_model_by_id(base_model_info)
    else:
        base_model_meta = None

    return user_define_operation(input_data_list=input_data_list,
                                 name=name,
                                 executor=executor,
                                 output_model=output_model_meta,
                                 base_model=base_model_meta,
                                 properties=exec_args,
                                 output_num=output_num)


def predict(input_data_list: List[Channel],
            model_info: Union[ModelMeta, Text, int],
            executor,
            model_version_info: Optional[Union[ModelVersionMeta, Text]] = None,
            exec_args: ExecuteArgs = None,
            output_num=1,
            name: Text = None) -> Union[NoneChannel, Channel, Tuple[Channel]]:
    """
    Predictor Operator. Do prediction job with the specific model version.

    :param input_data_list: List of Channel. It contains the example data used in prediction.
    :param model_info: Information about the model which is in prediction. Its type can be ModelMeta
                       of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The model_info
                       means name in he metadata service when its type is Text and it means id when its type is
                       int. The ai flow will get the model meta from metadata service by name or id.
    :param executor: The user defined function in predict operator. User can write their own logic here.
    :param model_version_info: Information about the model version which is in prediction. Its type can be
                               ModelVersionMeta of py:class:`ai_flow.meta.model_meta.ModelVersionMeta`
                               or Text. The model_version_info means version in he metadata service
                               when its type is Text. The ai flow will get the model meta from metadata
                               service by version.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param output_num: The output number of the operator. The default value is 1.
    :param name: Name of the predict operator.
    :return: Channel or Tuple[Channel].
    """
    if isinstance(model_info, ModelMeta):
        model_meta = model_info
    elif isinstance(model_info, Text):
        model_meta = get_ai_flow_client().get_model_by_name(model_info)
    else:
        model_meta = get_ai_flow_client().get_model_by_id(model_info)

    if isinstance(model_version_info, ModelVersionMeta):
        model_version_meta = model_version_info
    elif isinstance(model_version_info, Text):
        model_version_meta = get_ai_flow_client() \
            .get_model_version_by_version(model_version_info,
                                          get_ai_flow_client().get_model_by_name(model_meta.name).uuid)
    else:
        model_version_meta = None

    return user_define_operation(input_data_list=input_data_list,
                                 name=name,
                                 model=model_meta,
                                 executor=executor,
                                 model_version=model_version_meta,
                                 properties=exec_args,
                                 output_num=output_num)


def evaluate(input_data_list: List[Channel],
             model_info: Union[ModelMeta, Text, int],
             executor,
             exec_args: ExecuteArgs = None,
             output_num=0,
             name: Text = None) -> Union[NoneChannel, Channel, Tuple[Channel]]:
    """
    Evaluate Operator. Do evaluate job with the specific model version.

    :param input_data_list: List of Channel. It contains the example data used in prediction.
    :param model_info: Information about the model which is in prediction. Its type can be ModelMeta
                       of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The model_info
                       means name in he metadata service when its type is Text and it means id when its type is
                       int. The ai flow will get the model meta from metadata service by name or id.
    :param executor: The user defined function in evaluate operator. User can write their own logic here.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param output_num: The output number of the operator. The default value is 0.
    :param name: Name of the predict operator.
    :return: NoneChannel.
    """
    if isinstance(model_info, ModelMeta):
        model_meta = model_info
    elif isinstance(model_info, Text):
        model_meta = get_ai_flow_client().get_model_by_name(model_info)
    else:
        model_meta = get_ai_flow_client().get_model_by_id(model_info)

    return user_define_operation(input_data_list=input_data_list,
                                 name=name,
                                 model=model_meta,
                                 executor=executor,
                                 properties=exec_args,
                                 output_num=output_num)


def example_validate(input_data: Channel,
                     executor,
                     exec_args: ExecuteArgs = None,
                     name: Text = None
                     ) -> NoneChannel:
    """
    Example Validator Operator. Identifies anomalies in training and serving data in this operator.

    :param input_data: Channel. It contains the example data used in evaluation.
    :param executor: The user defined function in example validate operator. User can write their own logic here.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param name: Name of the example validate operator.
    :return: NoneChannel.
    """
    return user_define_operation(input_data_list=input_data,
                                 executor=executor,
                                 properties=exec_args,
                                 name=name,
                                 output_num=0
                                 )


def model_validate(input_data_list: List[Channel],
                   model_info: Union[ModelMeta, Text, int],
                   executor,
                   model_version_info: Optional[Union[ModelVersionMeta, Text]] = None,
                   base_model_version_info: Optional[Union[ModelVersionMeta, Text]] = None,
                   exec_args: ExecuteArgs = None,
                   output_num=0,
                   name: Text = None) -> Union[NoneChannel, Channel, Tuple[Channel]]:
    """
    Model Validator Operator. Compare the performance of two different versions of the same model and choose the better
    model version to make it ready to be in the stage of deployment.

    :param input_data_list: List of Channel. It contains the example data used in model validation.
    :param model_info: Information about the model which is in model validation. Its type can be ModelMeta
                       of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The model_info
                       means name in he metadata service when its type is Text and it means id when its type is
                       int. The ai flow will get the model meta from metadata service by name or id.
    :param executor: The user defined function in model validate operator. User can write their own logic here.
    :param model_version_info: Information about the model version which is in model validation. Its type can be
                               ModelVersionMeta of py:class:`ai_flow.meta.model_meta.ModelVersionMeta`
                               or Text. The model_version_info means version in he metadata service
                               when its type is Text. The ai flow will get the model meta from metadata
                               service by version.
    :param base_model_version_info: Information about the model version which is in model validation. Its type can be
                                    ModelVersionMeta of py:class:`ai_flow.meta.model_meta.ModelVersionMeta`
                                    or Text. The model_version_info means version in he metadata service
                                    when its type is Text. The ai flow will get the model meta from metadata
                                    service by version.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param output_num: The output number of the operator. The default value is 0.
    :param name: Name of the model validate operator.
    :return: NoneChannel.
    """
    if isinstance(model_info, ModelMeta):
        model_meta = model_info
    elif isinstance(model_info, Text):
        model_meta = get_ai_flow_client().get_model_by_name(model_info)
    else:
        model_meta = get_ai_flow_client().get_model_by_id(model_info)

    if isinstance(model_version_info, ModelVersionMeta):
        model_version_meta = model_version_info
    elif isinstance(model_version_info, Text):
        model_version_meta = get_ai_flow_client() \
            .get_model_version_by_version(model_version_info,
                                          get_ai_flow_client().get_model_by_name(model_meta.name).uuid)
    else:
        model_version_meta = None

    if isinstance(base_model_version_info, ModelVersionMeta):
        base_model_version_meta = model_version_info
    elif isinstance(base_model_version_info, Text):
        base_model_version_meta = get_ai_flow_client() \
            .get_model_version_by_version(model_version_info,
                                          get_ai_flow_client().get_model_by_name(model_meta.name).uuid)
    else:
        base_model_version_meta = None

    return user_define_operation(input_data_list=input_data_list,
                                 model=model_meta,
                                 executor=executor,
                                 properties=exec_args,
                                 name=name,
                                 model_version_meta=model_version_meta,
                                 base_model_version_meta=base_model_version_meta,
                                 output_num=output_num
                                 )


def push_model(model_info: Union[ModelMeta, Text, int],
               executor,
               model_version_info: Optional[Union[ModelVersionMeta, Text]] = None,
               exec_args: ExecuteArgs = None,
               name: Text = None) -> NoneChannel:
    """
    Pusher operator. The pusher operator is used to push a validated model which is better than previous one to
    a deployment target.

    :param model_info: Information about the model which is in pusher. Its type can be ModelMeta
                       of py:class:`ai_flow.meta.model_meta.ModelMeta` or Text or int. The model_info
                       means name in he metadata service when its type is Text and it means id when its type is
                       int. The ai flow will get the model meta from metadata service by name or id.
    :param executor: The user defined function in pusher operator. User can write their own logic here.
    :param model_version_info: Information about the model version which is in push. Its type can be
                               ModelVersionMeta of py:class:`ai_flow.meta.model_meta.ModelVersionMeta`
                               or Text. The model_version_info means version in he metadata service
                               when its type is Text. The ai flow will get the model meta from metadata
                               service by version.
    :param exec_args: The properties of read example, there are batch properties, stream properties and
                      common properties respectively.
    :param name: Name of the push operator.
    :return: NoneChannel.
    """
    if isinstance(model_info, ModelMeta) or model_info is None:
        model_meta = model_info
    elif isinstance(model_info, Text):
        model_meta = get_ai_flow_client().get_model_by_name(model_info)
    else:
        model_meta = get_ai_flow_client().get_model_by_id(model_info)

    if isinstance(model_version_info, ModelVersionMeta) or model_version_info is None:
        model_version_info = None
    elif isinstance(model_version_info, Text):
        model_version_info = get_ai_flow_client().get_model_version_by_version(version=model_version_info,
                                                                               model_id=model_meta.uuid)

    return user_define_operation(model=model_meta,
                                 executor=executor,
                                 properties=exec_args,
                                 model_version=model_version_info,
                                 name=name)


def user_define_operation(
        executor,
        input_data_list: Union[None, Channel, List[Channel]] = None,
        exec_args: ExecuteArgs = None,
        output_num=1,
        name: Text = None,
        **kwargs) -> Union[NoneChannel, Channel, Tuple[Channel]]:
    """
    User defined operator.

    :param executor: The user defined function in operator. User can write their own logic here.
    :param input_data_list: List of input data. It contains multiple channels from the operators which generate data.
    :param exec_args: The properties of read example
    :param output_num: The output number of the operator. The default value is 1.
    :param name: Name of this operator.
    :param kwargs:
    :return: NoneChannel or Channel or Tuple[Channel].
    """
    node = CustomAINode(name=name,
                        executor=executor,
                        exec_args=exec_args,
                        output_num=output_num,
                        **kwargs)
    add_ai_node_to_graph(node, inputs=input_data_list)
    outputs = node.outputs()
    if 0 == output_num:
        output: NoneChannel = outputs[0]
        return output
    else:
        if 1 == len(outputs):
            output: Channel = outputs[0]
            return output
        else:
            output: List[Channel] = []
            for i in outputs:
                tmp: Channel = i
                output.append(tmp)
            return tuple(output)


def stop_before_control_dependency(src: Channel,
                                   dependency: Channel,
                                   namespace: Text = DEFAULT_NAMESPACE
                                   ) -> None:
    """
    Add stop-before control dependency. It means src channel will start when and only the dependency channel stop.

    :param namespace:
    :param src: The src channel depended on the dependency channel.
    :param dependency: The channel which is the dependency.
    :return: None.
    """

    tmp = StopBeforeControlEdge(source_node_id=src.node_id,
                                target_node_id=dependency.node_id,
                                namespace=namespace)
    default_graph().add_edge(src.node_id, tmp)


def model_version_control_dependency(src: Channel,
                                     model_name: Text,
                                     model_version_event_type,
                                     dependency: Channel,
                                     namespace: Text = DEFAULT_NAMESPACE
                                     ) -> None:
    """
    Add model version control dependency. It means src channel will start when and only a new model version of the
    specific model is updated in notification service.

    :param namespace:
    :param model_version_event_type: one of ModelVersionEventType
    :param src: The src channel depended on the new model version which is updated in notification service.
    :param model_name: Name of the model, refers to a specific model.
    :param dependency: The channel which is the dependency.
    :return: None.
    """
    tmp = ModelVersionControlEdge(model_name=model_name,
                                  model_type=model_version_event_type,
                                  target_node_id=dependency.node_id,
                                  source_node_id=src.node_id,
                                  namespace=namespace)
    default_graph().add_edge(src.node_id, tmp)


def example_control_dependency(src: Channel,
                               example_name: Text,
                               dependency: Channel,
                               namespace: Text = DEFAULT_NAMESPACE
                               ) -> None:
    """
    Add example control dependency. It means src channel will start when and only the an new example of the specific
    example is updated in notification service.

    :param namespace: the namespace of the example
    :param src: The src channel depended on the example which is updated in notification service.
    :param example_name: Name of the example, refers to a specific example.
    :param dependency: The channel which is the dependency.
    :return: None.
    """
    tmp = ExampleControlEdge(example_name=example_name,
                             target_node_id=dependency.node_id,
                             source_node_id=src.node_id,
                             namespace=namespace)
    default_graph().add_edge(src.node_id, tmp)


def user_define_control_dependency(src: Channel,
                                   dependency: Channel,
                                   event_key: Text,
                                   event_value: Text,
                                   event_type: Text = UNDEFINED_EVENT_TYPE,
                                   condition: MetCondition = MetCondition.NECESSARY,
                                   action: TaskAction = TaskAction.START,
                                   life: EventLife = EventLife.ONCE,
                                   value_condition: MetValueCondition = MetValueCondition.EQUAL,
                                   namespace: Text = DEFAULT_NAMESPACE,
                                   sender: Text = None
                                   ) -> None:
    """
    Add user defined control dependency.

    :param namespace: the project name
    :param src: The src channel depended the event which is updated in notification service.
    :param dependency: The channel which is the dependency.
    :param event_key: The key of the event.
    :param event_value: The value of the event.
    :param event_type: The Name of the event.
    :param condition: The event condition. Sufficient or Necessary.
    :param action: The action act on the src channel. Start or Restart.
    :param life: The life of the event. Once or Repeated.
    :param value_condition: The event value condition. Equal or Update. Equal means the src channel will start or
                            restart only when in the condition that the notification service updates a value which
                            equals to the event value under the specific event key, while update means src channel
                            will start or restart when in the the condition that the notification service has a update
                            operation on the event key which event value belongs to.
    :param sender: The event sender identity. If sender is None, the sender will be dependency.
    :return:None.
    """
    control_edge = UserDefineControlEdge(target_node_id=dependency.node_id,
                                         source_node_id=src.node_id,
                                         event_key=event_key,
                                         event_value=event_value,
                                         event_type=event_type,
                                         condition=condition,
                                         action=action,
                                         life=life,
                                         value_condition=value_condition,
                                         namespace=namespace,
                                         sender=sender
                                         )
    default_graph().add_edge(src.node_id, control_edge)
