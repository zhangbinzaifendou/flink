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

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.CliOptions;
import org.apache.flink.table.client.cli.CliOptionsParser;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.table.client.config.entries.ConfigurationEntry.create;
import static org.apache.flink.table.client.config.entries.ConfigurationEntry.merge;

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
	private final CliOptions options;

	public static final String MODE_EMBEDDED = "embedded";
	public static final String MODE_GATEWAY = "gateway";

	public static final String DEFAULT_SESSION_ID = "default";

	public SqlClient(boolean isEmbedded, CliOptions options) {
		this.isEmbedded = isEmbedded;
		this.options = options;
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

			Map<String, String> map = new HashMap<String, String>();
			if (options.getMap() != null) {
				map = options.getMap();
			}

			final LocalExecutor executor = new LocalExecutor(options.getDefaults(), jars, libDirs);
			map.entrySet().stream().forEach(
				entry -> executor.getFlinkConfig().setString(entry.getKey(), entry.getValue()));

			executor.start();

			// create CLI client with session environment
			final Environment sessionEnv = Environment.enrich(readSessionEnvironment(options.getEnvironment()), map);
			appendPythonConfig(sessionEnv, options.getPythonConfiguration());
			final SessionContext context;
			if (options.getSessionId() == null) {
				context = new SessionContext(DEFAULT_SESSION_ID, sessionEnv);
			} else {
				context = new SessionContext(options.getSessionId(), sessionEnv);
			}

			// Open an new session
			String sessionId = executor.openSession(context);
			try {
				// add shutdown hook
				Runtime.getRuntime().addShutdownHook(new EmbeddedShutdownThread(sessionId, executor));

				// do the actual work
				openCli(sessionId, executor);
			} finally {
				executor.closeSession(sessionId);
			}
		} else {
			throw new SqlClientException("Gateway mode is not supported yet.");
		}
	}

	/**
	 * Opens the CLI client for executing SQL statements.
	 *
	 * @param sessionId session identifier for the current client.
	 * @param executor executor
	 */
	private void openCli(String sessionId, Executor executor) {
		CliClient cli = null;
		try {
			Path historyFilePath;
			if (options.getHistoryFilePath() != null) {
				historyFilePath = Paths.get(options.getHistoryFilePath());
			} else {
				historyFilePath = Paths.get(System.getProperty("user.home"),
						SystemUtils.IS_OS_WINDOWS ? "flink-sql-history" : ".flink-sql-history");
			}
			cli = new CliClient(sessionId, executor, historyFilePath);
			String savepointDir = ((LocalExecutor) executor).getFlinkConfig().getString(SavepointConfigOptions.SAVEPOINT_PATH);
			if (StringUtils.isNoneBlank(savepointDir)) {
				String sql  = null;
				Properties properties = new Properties();
				Map<String, String> variables = new HashedMap();
				List<String> functions = new ArrayList<>();
				List<String> tables = new ArrayList<>();
				try {
					properties =  paserSqlPropertiesFromSavePointMetaData(savepointDir);
					// 1 parse set execution variables
					variables = (Map<String, String>) properties.getOrDefault("flink.sql.variables", variables);
					// 2 parse function
					functions = (List<String>) properties.getOrDefault("flink.sql.functions", functions);
					// 3 parse create table
					tables = (List<String>) properties.getOrDefault("flink.sql.tables", tables);
					// 4 parse execution sql
					sql = properties.getProperty("flink.sql.string");
				} catch (Exception e) {
					throw new SqlClientException("parse sql from savepoint metadata error.", e);
				}
				cli.recoveryJobBySavepoint(variables, functions, tables, sql);
			} else {
				// interactive CLI mode
				if (options.getUpdateStatement() == null) {
					cli.open();
				}
				// execute single update statement
				else {
					final boolean success = cli.submitUpdate(options.getUpdateStatement());
					if (!success) {
						throw new SqlClientException("Could not submit given SQL update statement to cluster.");
					}
				}
			}
		} finally {
			if (cli != null) {
				cli.close();
			}
		}
	}

	private Properties paserSqlPropertiesFromSavePointMetaData(String savepointDir) throws IOException, ClassNotFoundException {
		Properties properties = new Properties();
		org.apache.flink.core.fs.Path path = new org.apache.flink.core.fs.Path(savepointDir);
		FileSystem fs = FileSystem.get(path.toUri());

		try (
			FSDataInputStream fsDataInputStream = fs.open(new org.apache.flink.core.fs.Path(path, AbstractFsCheckpointStorage.METADATA_FILE_NAME));
			DataInputStream dis = new DataInputStream(fsDataInputStream);
		){
			int magic = dis.readInt();
			int version = dis.readInt();
			int bytesLength = dis.readInt();
			if (bytesLength > 0) {
				byte[] bytes = new byte[bytesLength];
				dis.readFully(bytes);
				properties = byteArrayToObject(bytes);
			} else {
				throw new SqlClientException("savepoint dir " + savepointDir + " metadata does not contain sql string.");
			}
		}
		return properties;
	}

	private Properties byteArrayToObject(byte[] bytes) throws IOException, ClassNotFoundException {
		try (
			ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bais);
		) {
			return (Properties) ois.readObject();
		}
	}

	// --------------------------------------------------------------------------------------------

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

	private static void appendPythonConfig(Environment env, Configuration pythonConfiguration) {
		Map<String, Object> pythonConfig = new HashMap<>(pythonConfiguration.toMap());
		Map<String, Object> combinedConfig = new HashMap<>(merge(env.getConfiguration(), create(pythonConfig)).asMap());
		env.setConfiguration(combinedConfig);
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
				final String[] modeArgs = Arrays.copyOfRange(args, 1, args.length);
				final CliOptions options = CliOptionsParser.parseEmbeddedModeClient(modeArgs);
				if (options.isPrintHelp()) {
					CliOptionsParser.printHelpEmbeddedModeClient();
				} else {
					try {
						final SqlClient client = new SqlClient(true, options);
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

	private static class EmbeddedShutdownThread extends Thread {

		private final String sessionId;
		private final Executor executor;

		public EmbeddedShutdownThread(String sessionId, Executor executor) {
			this.sessionId = sessionId;
			this.executor = executor;
		}

		@Override
		public void run() {
			// Shutdown the executor
			System.out.println("\nShutting down the session...");
			executor.closeSession(sessionId);
			System.out.println("done.");
		}
	}
}
