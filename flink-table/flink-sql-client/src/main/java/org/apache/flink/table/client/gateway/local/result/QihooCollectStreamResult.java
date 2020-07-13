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

package org.apache.flink.table.client.gateway.local.result;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Collects results and returns them as table snapshots.
 *
 * @param <C> cluster id to which this result belongs to
 */
public class QihooCollectStreamResult<C> extends CollectStreamResult<C> implements MaterializedResult<C> {

	/** Maximum initial capacity of the materialized table. */
	public static final int MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY = 1_000_000;

	/** Maximum overcommitment of the materialized table. */
	public static final int MATERIALIZED_TABLE_MAX_OVERCOMMIT = 1_000_000;

	/** Factor for the initial capacity of the materialized table. */
	public static final double MATERIALIZED_TABLE_CAPACITY_FACTOR = 0.05;

	/** Factor for cleaning up deleted rows in the materialized table. */
	public static final double MATERIALIZED_TABLE_OVERCOMMIT_FACTOR = 0.01;

	/**
	 * Maximum number of materialized rows to be stored. After the count is reached, oldest
	 * rows are dropped.
	 */
	private final int maxRowCount;

	/** Threshold for cleaning up deleted rows in the materialized table. */
	private final int overcommitThreshold;

	/**
	 * Materialized table that is continuously updated by inserts and deletes. Deletes at
	 * the beginning are lazily cleaned up when the threshold is reached.
	 */
	private final List<Row> materializedTable;

	private final Map<Row, Integer> rowPositionCache;
	
	private CountDownLatch countDownLatch = new CountDownLatch(1);

	@VisibleForTesting
	public QihooCollectStreamResult(
			TableSchema tableSchema,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			int maxRowCount,
			int overcommitThreshold,
			ClassLoader classLoader) {
		super(tableSchema, config, gatewayAddress, gatewayPort, classLoader);

		if (maxRowCount <= 0) {
			this.maxRowCount = Integer.MAX_VALUE;
		} else {
			this.maxRowCount = maxRowCount;
		}

		this.overcommitThreshold = overcommitThreshold;

		// prepare for materialization
		final int initialCapacity = computeMaterializedTableCapacity(maxRowCount); // avoid frequent resizing
		materializedTable = new ArrayList<>(initialCapacity);
		rowPositionCache = new HashMap<>(initialCapacity);
	}

	public QihooCollectStreamResult(
			TableSchema tableSchema,
			ExecutionConfig config,
			InetAddress gatewayAddress,
			int gatewayPort,
			int maxRowCount,
			ClassLoader classLoader) {

		this(
			tableSchema,
			config,
			gatewayAddress,
			gatewayPort,
			maxRowCount,
			computeMaterializedTableOvercommit(maxRowCount),
			classLoader);
	}

	@Override
	public boolean isMaterialized() {
		return true;
	}

	@Override
	public TypedResult<Integer> snapshot(int pageSize) {
		return null;
	}

	@Override
	public List<Row> retrievePage(int page) {
		return null;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	protected void processRecord(Tuple2<Boolean, Row> change) {
		synchronized (resultLock) {
			// insert
			if (change.f0) {
				processInsert(change.f1);
			}
			// delete
			else {
				processDelete(change.f1);
			}
		}

		try {
			Thread.sleep(10);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private void processDelete(Row row) {
		// delete the newest record first to minimize per-page changes
		final Integer cachedPos = rowPositionCache.get(row);
		final int startSearchPos;
		if (cachedPos != null) {
			startSearchPos = Math.min(cachedPos, materializedTable.size() - 1);
		} else {
			startSearchPos = materializedTable.size() - 1;
		}

		for (int i = startSearchPos; i >= 0; i--) {
			if (materializedTable.get(i).equals(row)) {
				materializedTable.remove(i);
				rowPositionCache.remove(row);
				break;
			}
		}
	}
	
	@VisibleForTesting
	public List<Row> getMaterializedTable() {
		return materializedTable;
	}

	// --------------------------------------------------------------------------------------------

	private void processInsert(Row row) {
		// limit the materialized table
		if (materializedTable.size() >= maxRowCount) {
			cleanUp();
		}
		materializedTable.add(row);
		rowPositionCache.put(row, materializedTable.size() - 1);

		if (countDownLatch.getCount() == 1 ) {
			countDownLatch.countDown();
		}
	}

	private void cleanUp() {
		materializedTable.clear();
	}

	private static int computeMaterializedTableCapacity(int maxRowCount) {
		return Math.min(
			MATERIALIZED_TABLE_MAX_INITIAL_CAPACITY,
			Math.max(1, (int) (maxRowCount * MATERIALIZED_TABLE_CAPACITY_FACTOR)));
	}

	private static int computeMaterializedTableOvercommit(int maxRowCount) {
		return Math.min(
			MATERIALIZED_TABLE_MAX_OVERCOMMIT,
			(int) (maxRowCount * MATERIALIZED_TABLE_OVERCOMMIT_FACTOR));
	}

	public Tuple2<Object, List<Row>> getAllRows() {
		try {
			countDownLatch.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		if (executionException.get() != null) {
			throw executionException.get();
		}
		return new Tuple2<Object, List<Row>>(resultLock, materializedTable);
	}

	public void clearRowFromRowPositionCache(Row row) {
		rowPositionCache.remove(row);
	}

}
