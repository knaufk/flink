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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.util.MiniClusterResource;
import org.apache.flink.test.util.MiniClusterResourceConfiguration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Integration test for triggering and resuming from savepoints.
 */
@SuppressWarnings("serial")
public class JuhoSavepointRecoveryTest extends TestLogger {

	@Rule
	public final TemporaryFolder folder = new TemporaryFolder();


	@Test
	public void testRecovery() throws Exception {

		final Configuration config = new Configuration();

		config.setString("fs.hdfs.hadoopconf", "file:///home/knaufk/Documents/flink/flink-container/src/test/resources/");

		SavepointITCase.MiniClusterResourceFactory clusterFactory = new SavepointITCase.MiniClusterResourceFactory(
				1,
				80,
				config);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(80);
		env.setMaxParallelism(8000);
		env.setStateBackend(new RocksDBStateBackend("file:///tmp"));

		DataStream<Map<String, String>> stream = env.fromElements(Maps.newHashMap());

		stream.keyBy(new MapKeySelector("whatever"))
		      .timeWindow(
				      org.apache.flink.streaming.api.windowing.time.Time
						      .days(1))
		      .reduce(new DistinctFunction())
		      .setUidHash("19ede2f818524a7f310857e537fa6808");


		JobID jobId = JobID.generate();
		JobGraph jobGraph = env.getStreamGraph().getJobGraph(jobId);
		jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("file:///home/knaufk/Documents/ML/flink_debug_logs_3-savepoints/savepoint-54015d-12d47da510a2/_metadata", true));

		MiniClusterResource cluster = clusterFactory.get();
		cluster.before();
		ClusterClient<?> client = cluster.getClusterClient();
		//client.setDetached(true);
		JobSubmissionResult submissionResult = client.submitJob(
				jobGraph,
				JuhoSavepointRecoveryTest.class.getClassLoader());

		System.out.println(client.getJobStatus(jobId).get());

	}

	private static class MapKeySelector implements KeySelector<Map<String, String>, Object> {

		private final String[] fields;

		public MapKeySelector(String... fields) {
			this.fields = fields;
		}

		@Override
		public Object getKey(Map<String, String> event) throws Exception {
			Tuple key = Tuple.getTupleClass(fields.length).newInstance();
			for (int i = 0; i < fields.length; i++) {
				key.setField(event.getOrDefault(fields[i], ""), i);
			}
			return key;
		}
	}

	private static class DistinctFunction implements ReduceFunction<Map<String, String>> {

		private static final Logger LOG = LoggerFactory.getLogger(DistinctFunction.class);

		@Override
		public Map<String, String> reduce(Map<String, String> value1, Map<String, String> value2) {
			LOG.debug(
					"DistinctFunction.reduce returns: {}={}",
					value1.get("field"),
					value1.get("id"));
			return value1;
		}
	}

}
