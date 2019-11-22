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

package com.alibaba.flink.ml.lib.tensorflow.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


public class TableDebugRowSource implements StreamTableSource<Row>, Serializable {

    private RowTypeInfo typeInfo;
    private DebugRowSource debugRowSource;
    private static Logger LOG = LoggerFactory.getLogger(TableDebugRowSource.class);

    public TableDebugRowSource() {
        this(1);
    }

    public TableDebugRowSource(int dim) {
        this(dim, false);
    }
    public TableDebugRowSource(int dim, boolean hasString) {
        this.debugRowSource = new DebugRowSource(dim, hasString);
        this.typeInfo = debugRowSource.getTypeInfo();
    }



    @Override
    public TypeInformation<Row> getReturnType() {
        return typeInfo;
    }

    @Override
    public TableSchema getTableSchema() {
        return new TableSchema(typeInfo.getFieldNames(), typeInfo.getFieldTypes());
    }

    @Override
    public String explainSource() {
        return "debug_source";
    }

    @Override
    public DataStream<Row> getDataStream(StreamExecutionEnvironment execEnv) {
        return execEnv.addSource(debugRowSource)
            .name(explainSource());
    }

}
