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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.local.result.BasicResult;
import org.apache.flink.table.client.gateway.local.result.MaterializedResult;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.AbstractID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collects results using accumulators and returns them as table snapshots.
 */
public class QihooCollectBatchResult<C> extends BasicResult<C> implements MaterializedResult<C> {

	private final String accumulatorName;
	private final CollectBatchTableSink tableSink;
	private final Object resultLock;
	private final ClassLoader classLoader;

	private AtomicReference<SqlExecutionException> executionException = new AtomicReference<>();
	private List<Row> resultTable;
	private CountDownLatch countDownLatch = new CountDownLatch(1);

	public QihooCollectBatchResult(
		TableSchema tableSchema,
		ExecutionConfig config,
		ClassLoader classLoader) {

		accumulatorName = new AbstractID().toString();
		TypeSerializer<Row> serializer = tableSchema.toRowType().createSerializer(config);
		tableSink = new CollectBatchTableSink(accumulatorName, serializer, tableSchema);
		resultLock = new Object();
		this.classLoader = checkNotNull(classLoader);
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public void startRetrieval(JobClient jobClient) {
		jobClient.getJobExecutionResult(classLoader)
		.thenAccept(new ResultRetrievalHandler())
		.whenComplete((unused, throwable) -> {
			if (throwable != null) {
				executionException.compareAndSet(null,
					new SqlExecutionException(
						"Error while submitting job.",
						throwable));
			}
		});
	}

	@Override
	public TableSink<?> getTableSink() {
		return tableSink;
	}

	@Override
	public void close() {
	}

	@Override
	public List<Row> retrievePage(int page) {
		return null;
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		return null;
	}

	// --------------------------------------------------------------------------------------------

	private class ResultRetrievalHandler implements Consumer<JobExecutionResult> {

		@Override
		public void accept(JobExecutionResult jobExecutionResult) {
			try {
				final ArrayList<byte[]> accResult = jobExecutionResult.getAccumulatorResult(accumulatorName);
				if (accResult == null) {
					throw new SqlExecutionException("The accumulator could not retrieve the result.");
				}
				final List<Row> resultTable = SerializedListAccumulator.deserializeList(accResult, tableSink.getSerializer());
				// sets the result table all at once
				synchronized (resultLock) {
					QihooCollectBatchResult.this.resultTable = resultTable;
				}
			} catch (ClassNotFoundException | IOException e) {
				throw new SqlExecutionException("Serialization error while deserializing collected data.", e);
			} finally {
				countDownLatch.countDown();
			}
		}
	}

	public Tuple2<Object,List<Row>> getAllRows() {
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (executionException.get() != null) {
			throw executionException.get();
		}
		return new Tuple2<Object,List<Row>>(resultLock, resultTable);
	}
}
