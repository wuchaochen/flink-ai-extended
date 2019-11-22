/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.ml.examples.tensorflow.ut;

import com.alibaba.flink.ml.examples.tensorflow.mnist.MnistDataUtil;
import com.alibaba.flink.ml.examples.tensorflow.mnist.ops.MnistTFRPojo;
import com.alibaba.flink.ml.examples.tensorflow.ops.MnistTFRExtractPojoMapOp;
import com.alibaba.flink.ml.examples.tensorflow.ops.MnistTFRToRowTableSource;
import com.alibaba.flink.ml.operator.util.DataTypes;
import com.alibaba.flink.ml.tensorflow.client.TFConfig;
import com.alibaba.flink.ml.tensorflow.client.TFUtils;
import com.alibaba.flink.ml.tensorflow.coding.ExampleCoding;
import com.alibaba.flink.ml.tensorflow.coding.ExampleCodingConfig;
import com.alibaba.flink.ml.tensorflow.io.TFRecordSource;
import com.alibaba.flink.ml.tensorflow.util.TFConstants;
import com.alibaba.flink.ml.util.MLConstants;
import com.alibaba.flink.ml.util.SysUtil;
import org.apache.curator.test.TestingServer;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class TFMnistTest {
	private static TestingServer server;
	private static final String mnist_dist = "mnist_dist.py";
	private static final String mnist_dist_with_input = "mnist_dist_with_input.py";

	public TFMnistTest() {
	}

	@Before
	public void setUp() throws Exception {
		MnistDataUtil.prepareData();
		server = new TestingServer(2181, true);
	}

	@After
	public void tearDown() throws Exception {
		if (server != null) {
			server.stop();
		}
	}

	public TFConfig buildTFConfig(String pyFile) {
		return buildTFConfig(pyFile, String.valueOf(System.currentTimeMillis()));
	}

	private TFConfig buildTFConfig(String pyFile, String version) {
		System.out.println("Run Test: " + SysUtil._FUNC_());
		String rootPath = new File("").getAbsolutePath();
		String script = rootPath + "/src/test/python/" + pyFile;
		System.out.println("Current version:" + version);
		Map<String, String> properties = new HashMap<>();
		properties.put("batch_size", "32");
		properties.put("input", rootPath + "/target/data/train/");
		properties.put("epochs", "1");
		properties.put("checkpoint_dir", rootPath + "/target/ckpt/" + version);
		properties.put("export_dir", rootPath + "/target/export/" + version);
		return new TFConfig(2, 1, properties, script, "map_fun", null);
	}


	@Test
	public void testDataStreamApi() throws Exception {
		System.out.println("Run Test: " + SysUtil._FUNC_());
		TFConfig config = buildTFConfig(mnist_dist);
		StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		TFUtils.train(flinkEnv, config);
		JobExecutionResult result = flinkEnv.execute();
		System.out.println(result.getNetRuntime());
	}

	@Test
	public void testTableStreamApi() throws Exception {
		System.out.println("Run Test: " + SysUtil._FUNC_());
		StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		flinkEnv.setParallelism(2);
		TableEnvironment tableEnv = StreamTableEnvironment.create(flinkEnv);
		TFConfig config = buildTFConfig(mnist_dist);
		TFUtils.train(flinkEnv, tableEnv, null, config, null);

		flinkEnv.execute();
	}

	@Test
	public void testDataStreamHaveInput() throws Exception {
		System.out.println("Run Test: " + SysUtil._FUNC_());
		String version = String.valueOf(System.currentTimeMillis());
		StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		String rootPath = new File("").getAbsolutePath();
		String[] paths = new String[2];
		paths[0] = rootPath + "/target/data/train/0.tfrecords";
		paths[1] = rootPath + "/target/data/train/1.tfrecords";
		TFConfig config = buildTFConfig(mnist_dist_with_input, version);
		config.setWorkerNum(paths.length);
		TFRecordSource source = TFRecordSource.createSource(paths, 1);
		DataStream<byte[]> input = flinkEnv.addSource(source).setParallelism(paths.length);
		DataStream<MnistTFRPojo> pojoDataStream = input.flatMap(new MnistTFRExtractPojoMapOp())
				.setParallelism(input.getParallelism());
		setExampleCodingType(config);
		TFUtils.train(flinkEnv, pojoDataStream, config);
		JobExecutionResult result = flinkEnv.execute();
		System.out.println("Run Finish:" + result.getNetRuntime());

	}

	public static void setExampleCodingType(TFConfig config) {
		String[] names = { "image_raw", "label" };
		DataTypes[] types = { DataTypes.STRING, DataTypes.INT_32 };
		String str = ExampleCodingConfig.createExampleConfigStr(names, types,
				ExampleCodingConfig.ObjectType.POJO, MnistTFRPojo.class);
		config.getProperties().put(TFConstants.INPUT_TF_EXAMPLE_CONFIG, str);
		config.getProperties().put(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, str);
		config.getProperties().put(MLConstants.ENCODING_CLASS, ExampleCoding.class.getCanonicalName());
		config.getProperties().put(MLConstants.DECODING_CLASS, ExampleCoding.class.getCanonicalName());
	}

	public static void setExampleCodingRowType(TFConfig config) {
		String[] names = { "image_raw", "label" };
		DataTypes[] types = { DataTypes.STRING, DataTypes.INT_32 };
		String str = ExampleCodingConfig.createExampleConfigStr(names, types,
				ExampleCodingConfig.ObjectType.ROW, MnistTFRPojo.class);
		config.getProperties().put(TFConstants.INPUT_TF_EXAMPLE_CONFIG, str);
		config.getProperties().put(TFConstants.OUTPUT_TF_EXAMPLE_CONFIG, str);
		config.getProperties().put(MLConstants.ENCODING_CLASS, ExampleCoding.class.getCanonicalName());
		config.getProperties().put(MLConstants.DECODING_CLASS, ExampleCoding.class.getCanonicalName());
	}

	@Test
	public void testTableStreamHaveInput() throws Exception {
		System.out.println("Run Test: " + SysUtil._FUNC_());
		String version = String.valueOf(System.currentTimeMillis());
		TFConfig config = buildTFConfig(mnist_dist_with_input, version);
		setExampleCodingRowType(config);
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(2);
		TableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv);
		String rootPath = new File("").getAbsolutePath();
		String[] paths = new String[2];
		paths[0] = rootPath + "/target/data/train/0.tfrecords";
		paths[1] = rootPath + "/target/data/train/1.tfrecords";
		tableEnv.registerTableSource("input", new MnistTFRToRowTableSource(paths, 1));
		Table inputTable = tableEnv.scan("input");
		TFUtils.train(streamEnv, tableEnv, inputTable, config, null);
		streamEnv.execute();
	}

}
