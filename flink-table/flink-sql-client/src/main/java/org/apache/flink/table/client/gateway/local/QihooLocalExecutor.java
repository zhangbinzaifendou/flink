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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogFunctionImpl;
import org.apache.flink.table.catalog.FunctionLanguage;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.result.DynamicResult;
import org.apache.flink.table.client.gateway.local.result.QihooCollectBatchResult;
import org.apache.flink.table.client.gateway.local.result.QihooCollectStreamResult;
import org.apache.flink.table.operations.ddl.CreateFunctionOperation;
import org.apache.flink.table.operations.ddl.DropFunctionOperation;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.List;

public class QihooLocalExecutor extends LocalExecutor {


	private SessionContext sessionContext;
	/**
	 * Creates a local executor for submitting table programs and retrieving results.
	 */
	public QihooLocalExecutor(URL defaultEnv, List<URL> jars, List<URL> libraries) {
		super(defaultEnv, jars, libraries);
	}

	public void addFunction(String sessionId, String name, String functionClassName, String language) throws Exception {
		ExecutionContext<?> context = getExecutionContext(sessionId);
		TableEnvironment tableEnv = context.getTableEnvironment();
		String currentCatalogName = tableEnv.getCurrentCatalog();
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(currentCatalogName, tableEnv.getCurrentDatabase(), name);
		CatalogFunction catalogFunction = new CatalogFunctionImpl(functionClassName,
			null==language || language.equalsIgnoreCase("java")? FunctionLanguage.JAVA : FunctionLanguage.SCALA);
		CreateFunctionOperation  createFunctionOperation =  new CreateFunctionOperation(objectIdentifier, catalogFunction, false, false);
		Catalog catalog = tableEnv.getCatalog(currentCatalogName).orElseThrow(
			() -> new ValidationException(String.format("Catalog %s does not exist", currentCatalogName)));

		catalog.createFunction(
			createFunctionOperation.getFunctionIdentifier().toObjectPath(),
			createFunctionOperation.getCatalogFunction(),
			createFunctionOperation.isIgnoreIfExists());
	}

	public void removeFunction(String sessionId, String name) throws Exception {
		ExecutionContext<?> context = getExecutionContext(sessionId);
		TableEnvironment tableEnv = context.getTableEnvironment();
		String currentCatalogName = tableEnv.getCurrentCatalog();
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(currentCatalogName, tableEnv.getCurrentDatabase(), name);
		DropFunctionOperation dropFunctionOperation = new DropFunctionOperation(objectIdentifier, false, false);

		Catalog catalog = tableEnv.getCatalog(currentCatalogName).orElseThrow(
				() -> new ValidationException(String.format("Catalog %s does not exist", currentCatalogName)));

		catalog.dropFunction(
			dropFunctionOperation.getFunctionIdentifier().toObjectPath(),
			dropFunctionOperation.isIfExists());
	}

	@Override
	public String openSession(SessionContext sessionContext) throws SqlExecutionException {
		this.sessionContext = sessionContext;
		return super.openSession(sessionContext);
	}

	@Override
	public void setSessionProperty(String sessionId, String key, String value) throws SqlExecutionException {
		if (key!=null && key.equals("execution.current-catalog")) {
			useCatalog(sessionId, value);
		} else if (key!=null && key.equals("execution.current-database")) {
			useDatabase(sessionId, value);
		}
		super.setSessionProperty(sessionId, key, value);
	}

	public Tuple2<Object,List<Row>> getAllRow(String resultId) throws SqlExecutionException {
		if (getContextMap().get(sessionContext.getSessionId()).getEnvironment().getExecution().inStreamingMode()) {
			return ((QihooCollectStreamResult<?>) getResult(resultId)).getAllRows();
		}
		return ((QihooCollectBatchResult<?>) getResult(resultId)).getAllRows();
	}

	public DynamicResult<?> getResult(String resultId) {
		final DynamicResult<?> result = getResultStore().getResult(resultId);
		if (result == null) {
			throw new SqlExecutionException("Could not find a result with result identifier '" + resultId + "'.");
		}
		if (!result.isMaterialized()) {
			throw new SqlExecutionException("Invalid result retrieval mode.");
		}
		return result;
	}
}
