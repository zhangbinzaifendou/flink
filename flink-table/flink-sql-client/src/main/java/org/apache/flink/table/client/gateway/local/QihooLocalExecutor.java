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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.client.cli.PrivateUtil;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.QihooCollectStreamResult;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.ddl.AlterCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.CreateTableOperation;
import org.apache.flink.table.operations.ddl.CreateTempSystemFunctionOperation;
import org.apache.flink.table.operations.ddl.DropCatalogFunctionOperation;
import org.apache.flink.table.operations.ddl.DropTempSystemFunctionOperation;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class QihooLocalExecutor extends LocalExecutor {


	private SessionContext sessionContext;

	private Properties properties = new Properties();
	/**
	 * Creates a local executor for submitting table programs and retrieving results.
	 */
	public QihooLocalExecutor(URL defaultEnv, List<URL> jars, List<URL> libraries) {
		super(defaultEnv, jars, libraries);
	}

	@Override
	public String openSession(SessionContext sessionContext) throws SqlExecutionException {
		this.sessionContext = sessionContext;
		String sessionId = sessionContext.getSessionId();

		try {
			if (getExecutionContext(sessionId)!= null) {
				throw new RuntimeException("Found another session with the same session identifier: " + sessionId);
			}
		} catch (SqlExecutionException e) {
		}
		ExecutionContext.Builder builder = (ExecutionContext.Builder) PrivateUtil.invokeParentMethod(
			this,
			"createExecutionContextBuilder",
			new Class[]{SessionContext.class},
			new Object[]{sessionContext});

		ExecutionContext context = builder.build();

		try {
			StreamExecutionEnvironment env = (StreamExecutionEnvironment) PrivateUtil.getParentPrivateVar(
				context.getClass(),
				"streamExecEnv").get(context);
			env.getCheckpointConfig().configure(context.getFlinkConfig());

			ConcurrentHashMap<String, ExecutionContext<?>> contextMap = (ConcurrentHashMap<String, ExecutionContext<?>>)
				PrivateUtil.getParentPrivateVar(
					LocalExecutor.class,
					"contextMap").get(this);

			contextMap.put(
				sessionId,
				context);
		} catch (Exception e) {
			throw new SqlExecutionException(e.getMessage());
		}
		return sessionId;
	}

	@Override
	public void setSessionProperty(String sessionId, String key, String value) throws SqlExecutionException {
		if (key!=null && key.equals("execution.current-catalog")) {
			useCatalog(sessionId, value);
		} else if (key!=null && key.equals("execution.current-database")) {
			useDatabase(sessionId, value);
		}
		Map<String, String> map = (Map<String, String>) properties.getOrDefault(
			"flink.sql.variables",
				 new HashMap<String, String>());

		if (map.size() == 0) {
			properties.put("flink.sql.variables", map);
		}
		map.put(key, value);
		super.setSessionProperty(sessionId, key, value);
	}

	public Tuple2<Object,List<Row>> getAllRow(String resultId) throws SqlExecutionException {
		if (getExecutionContext(sessionContext.getSessionId()).getEnvironment().getExecution().inStreamingMode()) {
			return ((QihooCollectStreamResult<?>) getResult(resultId)).getAllRows();
		}
		return ((QihooCollectBatchResult<?>) getResult(resultId)).getAllRows();
	}

	public DynamicResult<?> getResult(String resultId) {
		DynamicResult<?> result = null;
		try {
			ResultStore resultStore = (ResultStore) PrivateUtil.getParentPrivateVar(
				LocalExecutor.class,
				"resultStore").get(this);
			result = resultStore.getResult(resultId);
			if (result == null) {
				throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
			}
			if (!result.isMaterialized()) {
				throw new SqlExecutionException("Invalid result retrieval mode.");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	@Override
	public ResultDescriptor executeQuery(String sessionId, String query) throws SqlExecutionException {
		CheckpointConfig checkpointConfig = getExecutionContext(sessionId).getStreamExecEnv().getCheckpointConfig();
		properties.put("flink.sql.string", Base64.getEncoder().encodeToString(query.getBytes()));
		checkpointConfig.setProperties(properties);
		return super.executeQuery(sessionId, query);
	}

	@Override
	public ProgramTargetDescriptor executeUpdate(String sessionId, String statement) throws SqlExecutionException {
		CheckpointConfig checkpointConfig = getExecutionContext(sessionId).getStreamExecEnv().getCheckpointConfig();
		properties.put("flink.sql.string", Base64.getEncoder().encodeToString(statement.getBytes()));
		checkpointConfig.setProperties(properties);
		return super.executeUpdate(sessionId, statement);
	}


	@Override
	public TableResult executeSql(String sessionId, String statement) throws SqlExecutionException {
		final ExecutionContext<?> context = getExecutionContext(sessionId);
		final TableEnvironment tEnv = context.getTableEnvironment();
		try {
			return context.wrapClassLoader(() -> {
					List<Operation> operations = getSqlParser(sessionId).parse(statement);
					// set create table to checkpoint metadata
					if (operations.get(0) instanceof CreateTableOperation) {
						List<String> list = (List<String>) properties.getOrDefault(
										"flink.sql.tables",
											  new ArrayList<String>());
						if (list.size() == 0) {
							properties.put("flink.sql.tables", list);
						}
						list.add(new String(Base64.getEncoder().encode(statement.getBytes())));
					} else if (operations.get(0) instanceof CreateCatalogFunctionOperation ||
						operations.get(0) instanceof CreateTempSystemFunctionOperation ||
						operations.get(0) instanceof DropCatalogFunctionOperation ||
						operations.get(0) instanceof DropTempSystemFunctionOperation ||
						operations.get(0) instanceof AlterCatalogFunctionOperation) {
						List<String> list = (List<String>) properties.getOrDefault(
							"flink.sql.functions",
							new ArrayList<String>());
						if (list.size() == 0) {
							properties.put("flink.sql.functions", list);
						}
						list.add(new String(Base64.getEncoder().encode(statement.getBytes())));
					}
					TableResult result = tEnv.executeSql(statement);
					return result;
				}
			);
		} catch (Exception e) {
			throw new SqlExecutionException("Could not execute statement: " + statement, e);
		}
	}

	public Properties getProperties() {
		return properties;
	}
}
