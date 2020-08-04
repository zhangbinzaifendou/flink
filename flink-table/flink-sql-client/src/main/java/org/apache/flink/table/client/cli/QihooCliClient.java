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

package org.apache.flink.table.client.cli;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.SqlCommandParser.SqlCommandCall;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.local.QihooLocalExecutor;
import org.apache.flink.table.client.gateway.local.result.QihooCollectStreamResult;
import org.apache.flink.types.Row;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.MaskingCallback;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * SQL CLI client.
 */
public class QihooCliClient extends CliClient {

	private static final Logger LOG = LoggerFactory.getLogger(QihooCliClient.class);

	private boolean isRunning;

	/**
	 * Creates a CLI instance with a custom terminal. Make sure to close the CLI instance
	 * afterwards using {@link #close()}.
	 */
	@VisibleForTesting
	public QihooCliClient(
		Terminal terminal,
		String sessionId,
		Executor executor,
		Path historyFilePath) {
		super(terminal, sessionId, executor, historyFilePath);
	}

	/**
	 * Creates a CLI instance with a prepared terminal. Make sure to close the CLI instance
	 * afterwards using {@link #close()}.
	 */
	public QihooCliClient(String sessionId, Executor executor, Path historyFilePath) {
		this(createDefaultTerminal(), sessionId, executor, historyFilePath);
	}

	/**
	 * Opens the interactive CLI shell.
	 */
	@Override
	public void open() {
		isRunning = true;
		LineReader lineReader = null;
		String prompt = "";

		try {
			lineReader = (LineReader) PrivateUtil.getParentPrivateVar(CliClient.class, "lineReader").get(this);
			prompt = (String) PrivateUtil.getParentPrivateVar(CliClient.class, "prompt").get(this);
		} catch (Exception e) {
			throw new SqlClientException("Could not init lineReader or prompt.", e);
		}
		// print welcome
		getTerminal().writer().append(CliStrings.MESSAGE_WELCOME);

		// begin reading loop
		while (isRunning) {
			// make some space to previous command
			getTerminal().writer().append("\n");
			getTerminal().flush();

			final String line;
			try {
				line = lineReader.readLine(prompt, null, (MaskingCallback) null, null);
			} catch (UserInterruptException e) {
				// user cancelled line with Ctrl+C
				continue;
			} catch (EndOfFileException | IOError e) {
				// user cancelled application with Ctrl+D or kill
				break;
			} catch (Throwable t) {
				throw new SqlClientException("Could not read from command line.", t);
			}
			if (line == null) {
				continue;
			}
			final List<Optional<SqlCommandCall>> calls = parseCommands(line);
			for (Optional<SqlCommandCall> call : calls) {
				call.ifPresent(this::callCommand);
			}
		}
	}


	private List<Optional<SqlCommandCall>> parseCommands(String lines) {
		List<Optional<SqlCommandCall>> parsedLineList = new ArrayList<>();
		for (String line : lines.split(";")) {
			parsedLineList.add(
				(Optional<SqlCommandCall>) PrivateUtil.invokeParentMethod(
					this,
					"parseCommand",
					new Class[]{String.class},
					new Object[]{line}
				)
			);
		}
		return parsedLineList;
	}

	private void callCommand(SqlCommandCall cmdCall) {
		switch (cmdCall.command) {
			case USE_CATALOG:
				callUseCatalog(cmdCall);
				break;
			case USE:
				callUseDatabase(cmdCall);
				break;
			case SELECT:
				callSelect(cmdCall);
				break;
			case QUIT:
				callQuit();
				break;
			default:
				// invoke CliClient callCommand
				PrivateUtil.invokeParentMethod(this, "callCommand", new Class[]{SqlCommandCall.class}, new Object[]{cmdCall});

		}
	}

	private void callSelect(SqlCommandCall cmdCall) {
		final ResultDescriptor resultDesc;
		try {
			resultDesc = getExecutor().executeQuery(getSessionId(), cmdCall.operands[0]);
		} catch (SqlExecutionException e) {
			printException("exec sql " + cmdCall.operands[0], e);
			return;
		}
		Tuple2<Object, List<Row>> tuple2 = null;
		List<Row> allRow = null;
		Object resultLock = null;
		try {
			tuple2 = ((QihooLocalExecutor) getExecutor()).getAllRow(resultDesc.getResultId());
			resultLock = tuple2.f0;
			allRow = tuple2.f1;
			if (allRow == null) {
				getTerminal().writer().println(CliStrings.messageInfo(CliStrings.MESSAGE_EMPTY).toAnsi());
			} else {
				if (((QihooLocalExecutor) getExecutor()).getExecutionContext(getSessionId()).getEnvironment().getExecution().inStreamingMode()) {
					while (true) {
						synchronized (resultLock) {
							allRow.removeIf(
								v -> {
									getTerminal().writer().println("result:" + v);
									getTerminal().flush();
									((QihooCollectStreamResult<?>) ((QihooLocalExecutor) getExecutor()).getResult(resultDesc.getResultId())).clearRowFromRowPositionCache(v);
									return true;
								}
							);
						}
						Thread.sleep(5000);
					}
				} else {
					allRow.forEach(
						(v) -> {
							getTerminal().writer().println("result:" + v);
							getTerminal().flush();
						}
					);
				}
			}
		} catch (Exception e) {
			getTerminal().writer().println(e.getCause().getCause());
		}
		getTerminal().flush();
	}

	public void callQuit() {
		printInfo(CliStrings.MESSAGE_QUIT);
		isRunning = false;
	}


	private void callUseCatalog(SqlCommandCall cmdCall) {
		PrivateUtil.invokeParentMethod(this, "callUseCatalog", new Class[]{SqlCommandCall.class}, new Object[]{cmdCall});
		PrivateUtil.invokeParentMethod(
			getExecutor(),
			"setSessionProperty",
			new Class[]{String.class, String.class, String.class},
			new Object[]{getSessionId(), "execution.current-catalog", cmdCall.operands[0]});
	}

	private void callUseDatabase(SqlCommandCall cmdCall) {
		PrivateUtil.invokeParentMethod(this, "callUseDatabase", new Class[]{SqlCommandCall.class}, new Object[]{cmdCall});
		PrivateUtil.invokeParentMethod(
			getExecutor(),
			"setSessionProperty",
			new Class[]{String.class, String.class, String.class},
			new Object[]{getSessionId(), "execution.current-database", cmdCall.operands[0]});
	}

	@Override
	public void printError(String message) {
		super.printError(message);
		System.exit(1);
	}

	@Override
	public void printException(String message, Throwable t) {
		super.printException(message, t);
		System.exit(1);
	}

	@Override
	public void printExecutionError(String message) {
		super.printExecutionError(message);
		System.exit(1);
	}

	private void printInfo(String message) {
		getTerminal().writer().println(CliStrings.messageInfo(message).toAnsi());
		getTerminal().flush();
	}
	// --------------------------------------------------------------------------------------------

	private static Terminal createDefaultTerminal() {
		try {
			return TerminalBuilder.builder()
				.name(CliStrings.CLI_NAME)
				.build();
		} catch (IOException e) {
			throw new SqlClientException("Error opening command line interface.", e);
		}
	}
}
