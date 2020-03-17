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

package org.apache.flink.table.client;

import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SQL Client for submitting SQL statements. The client can be executed in two
 * modes: a gateway and embedded mode.
 *
 * <p>- In embedded mode, the SQL CLI is tightly coupled with the executor in a common process. This
 * allows for submitting jobs without having to start an additional component.
 *
 * <p>- In future versions: In gateway mode, the SQL CLI client connects to the REST API of the gateway
 * and allows for managing queries via console.
 *
 * <p>For debugging in an IDE you can execute the main method of this class using:
 * "embedded --defaults /path/to/sql-client-defaults.yaml --jar /path/to/target/flink-sql-client-*.jar"
 *
 * <p>Make sure that the FLINK_CONF_DIR environment variable is set.
 */
public class SqlClient {

	private static final Logger LOG = LoggerFactory.getLogger(SqlClient.class);

	private final boolean isEmbedded;
	private boolean isXSQL = false;
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";
	public static final String XSQL = "xsql";

	public static final String DEFAULT_SESSION_ID = "default";

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
	}

	public SqlClient(boolean isEmbedded, boolean isXSQL, CliOptions options) {
		this(isEmbedded, options);
		this.isXSQL = isXSQL;
	}

	private void start() {
		if (isEmbedded) {
			// create local executor with default environment
			final List<URL> jars;
			if (options.getJars() != null) {
				jars = options.getJars();
			} else {
				jars = Collections.emptyList();
			}
			final List<URL> libDirs;
			if (options.getLibraryDirs() != null) {
				libDirs = options.getLibraryDirs();
			} else {
				libDirs = Collections.emptyList();
			}

			final String sql;
			if (options.getSql() != null) {
				sql = options.getSql();
			} else {
				sql = "";
			}

			final String sqlFile;
			if (options.getSqlFile() != null) {
				sqlFile = options.getSqlFile();
			} else {
				sqlFile = "";
			}

			Map<String, String> map = new HashMap<String, String>();
			if (options.getMap() != null) {
				map = options.getMap();
			}

			final LocalExecutor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
			//merge command --conf
			executor.getFlinkConfig().addAll(map);
			executor.start();

			// create CLI client with session environment
			final Environment sessionEnv = Environment.enrich(
				readSessionEnvironment(options.getEnvironment()), map, new HashMap<>());
			final SessionContext context;
			if (options.getSessionId() == null) {
				context = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
			} else {
				context = new SessionContext(options.getSessionId(), sessionEnv);
			}

			// validate the environment (defaults and session)
			validateEnvironment(context, executor);

			// add shutdown hook
			Runtime.getRuntime().addShutdownHook(new EmbeddedShutdownThread(context, executor));

			// do the actual work
			if (!StringUtils.isNullOrWhitespaceOnly(sql)) {
				executeSql(context, executor, sql);
			} else if (!StringUtils.isNullOrWhitespaceOnly(sqlFile)) {
				executeSqlFile(context, executor, sqlFile);
			} else {
				openCli(context, executor);
			}
		} else {
			throw new SqlClientException("Gateway mode is not supported yet.");
		}
	}

	private void executeSql(SessionContext context, Executor executor, String sql) {
		final CliClient cli = new CliClient(context, executor);
		cli.executeSql(sql);
		cli.callQuit();
	}

	private void executeSqlFile(SessionContext context, Executor executor, String sql) {
		final CliClient cli = new CliClient(context, executor);
		cli.executeSqlFile(sql);
		cli.callQuit();
	}

	/**
	 * Opens the CLI client for executing SQL statements.
	 *
	 * @param context  session context
	 * @param executor executor
	 */
	private void openCli(SessionContext context, Executor executor) {
		CliClient cli = null;
		try {
			cli = new CliClient(context, executor);
			// interactive CLI mode
			if (options.getUpdateStatement() == null) {
				if (isXSQL) {
					cli.open4XSQL();
				} else {
					cli.open();
				}
			}
			// execute single update statement
			else {
				final boolean success = cli.submitUpdate(options.getUpdateStatement());
				if (!success) {
					throw new SqlClientException("Could not submit given SQL update statement to cluster.");
				}
			}
		} finally {
			if (cli != null) {
				cli.close();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private static void validateEnvironment(SessionContext context, Executor executor) {
		System.out.print("Validating current environment...");
		try {
			executor.validateSession(context);
			System.out.println("done.");
		} catch (SqlExecutionException e) {
			throw new SqlClientException(
				"The configured environment is invalid. Please check your environment files again.", e);
		}
	}

	private static void shutdown(SessionContext context, Executor executor) {
		System.out.println();
		System.out.println("Shutting down executor...");
		executor.stop(context);
	}

	private static Environment readSessionEnvironment(URL envUrl) {
		// use an empty environment by default
		if (envUrl == null) {
			System.out.println("No session environment specified.");
			return new Environment();
		}

		System.out.println("Reading session environment from: " + envUrl);
		LOG.info("Using session environment file: {}", envUrl);
		try {
			return Environment.parse(envUrl);
		} catch (IOException e) {
			throw new SqlClientException("Could not read session environment file at: " + envUrl, e);
		}
	}

	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) {
		if (args.length < 1) {
			CliOptionsParser.printHelpClient();
			return;
		}

		switch (args[0]) {

			case MODE_EMBEDDED:
				// remove mode
				String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
				// xsql
				boolean isXSQL = false;
				if (modeArgs != null && modeArgs.length > 0 && modeArgs[0].equals(XSQL)) {
					isXSQL = true;
					modeArgs = Arrays.copyOfRange(modeArgs, 1, modeArgs.length);
				}
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
				} else {
					try {
						final SqlClient client = new SqlClient(true, isXSQL, options);
						client.start();
					} catch (SqlClientException e) {
						// make space in terminal
						System.out.println();
						System.out.println();
						LOG.error("SQL Client must stop.", e);
						throw e;
					} catch (Throwable t) {
						// make space in terminal
						System.out.println();
						System.out.println();
						LOG.error("SQL Client must stop. Unexpected exception. This is a bug. Please consider filing an issue.", t);
						throw new SqlClientException("Unexpected exception. This is a bug. Please consider filing an issue.", t);
					}
				}
				break;

			case MODE_GATEWAY:
				throw new SqlClientException("Gateway mode is not supported yet.");

			default:
				CliOptionsParser.printHelpClient();
		}
	}

	// --------------------------------------------------------------------------------------------

	private class EmbeddedShutdownThread extends Thread {

		private final SessionContext context;
		private final Executor executor;

		public EmbeddedShutdownThread(SessionContext context, Executor executor) {
			this.context = context;
			this.executor = executor;
		}

		@Override
		public void run() {
			shutdown(context, executor);
		}
	}
}
