package com.lordofthejars.nosqlunit.elasticsearch6;

import static com.lordofthejars.nosqlunit.core.IOUtils.deleteDir;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;
import com.lordofthejars.nosqlunit.core.CommandLineExecutor;
import com.lordofthejars.nosqlunit.core.OperatingSystem;
import com.lordofthejars.nosqlunit.core.OperatingSystemFamily;
import com.lordofthejars.nosqlunit.core.OperatingSystemResolver;
import com.lordofthejars.nosqlunit.core.OsNameSystemPropertyOperatingSystemResolver;
import com.lordofthejars.nosqlunit.env.SystemEnvironmentVariables;

public class ManagedElasticsearchLifecycleManager extends AbstractLifecycleManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(ManagedElasticsearchLifecycleManager.class);

	private static final Settings DEFAULT_SETTINGS = Settings.EMPTY;
	private static final String DEFAULT_HOST = "localhost";
	protected static final int DEFAULT_PORT = 9300;
	protected static final String DEFAULT_ELASTICSEARCH_TARGET_PATH = createTempEsFolder();
	protected static final String FOREGROUND_OPTION = "-f";
	private static final String ELASTICSEARCH_BINARY_DIRECTORY = "bin";

	private static final String ELASTICSEARCH_EXECUTABLE_X = "elasticsearch";
	private static final String ELASTICSEARCH_EXECUTABLE_W = "elasticsearch.bat";

	private Map<String, String> extraCommandArguments = new HashMap<String, String>();
	private List<String> singleCommandArguments = new ArrayList<String>();

	private CommandLineExecutor commandLineExecutor = new CommandLineExecutor();
	private OperatingSystemResolver operatingSystemResolver = new OsNameSystemPropertyOperatingSystemResolver();
	private LowLevelElasticSearchOperations lowLevelElasticSearchOperations = new LowLevelElasticSearchOperations();

	private String elasticsearchPath = SystemEnvironmentVariables.getEnvironmentOrPropertyVariable("ES_HOME");
	private int port = DEFAULT_PORT;

	private String targetPath = DEFAULT_ELASTICSEARCH_TARGET_PATH;

	private ProcessRunnable processRunnable;

	public ManagedElasticsearchLifecycleManager() {
		super();
	}

	@Override
	public String getHost() {
		return DEFAULT_HOST;
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public void doStart() throws Throwable {

		LOGGER.info("Starting {} Elasticsearch instance.", elasticsearchPath);

		File dbPath = ensureDbPathDoesNotExitsAndReturnCompositePath();

		if (dbPath.mkdirs()) {
			startElasticsearchAsADaemon();
			boolean isServerUp = this.lowLevelElasticSearchOperations
					.assertThatConnectionToElasticsearchIsPossible(DEFAULT_SETTINGS, DEFAULT_HOST, DEFAULT_PORT);

			if (!isServerUp) {
				throw new IllegalStateException("Couldn't establish a connection with " + this.elasticsearchPath
						+ " server at /127.0.0.1:" + port);
			}

		} else {
			throw new IllegalStateException("Db Path " + dbPath + " could not be created.");
		}

		LOGGER.info("Started {} Elasticsearch instance.", elasticsearchPath);
	}

	private void startElasticsearchAsADaemon() throws InterruptedException {
		CountDownLatch processIsReady = new CountDownLatch(1);
		processRunnable = new ProcessRunnable(processIsReady);
		Thread thread = new Thread(processRunnable);
		thread.start();
		processIsReady.await();
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping {} Elasticsearch instance.", elasticsearchPath);

		try {
			if (this.processRunnable != null) {
				this.processRunnable.destroyProcess();
			}
		} finally {
			ensureDbPathDoesNotExitsAndReturnCompositePath();
		}

		LOGGER.info("Stopped {} Elasticsearch instance.", elasticsearchPath);
	}

	private List<String> buildOperationSystemProgramAndArguments() {

		List<String> programAndArguments = new ArrayList<String>();

		programAndArguments.add(getExecutablePath());

		if (isXBased()) {
			programAndArguments.add(FOREGROUND_OPTION);
		}

		for (String argument : this.singleCommandArguments) {
			programAndArguments.add(argument);
		}

		for (String argumentName : this.extraCommandArguments.keySet()) {
			programAndArguments.add(argumentName);
			programAndArguments.add(this.extraCommandArguments.get(argumentName));
		}

		return programAndArguments;

	}

	private boolean isXBased() {
		OperatingSystemFamily family = this.operatingSystemResolver.currentOperatingSystem().getFamily();
		return family != OperatingSystemFamily.WINDOWS;
	}

	private String getExecutablePath() {
		return this.elasticsearchPath + File.separatorChar + ELASTICSEARCH_BINARY_DIRECTORY + File.separatorChar
				+ elasticsearchExecutable();
	}

	private File ensureDbPathDoesNotExitsAndReturnCompositePath() {
		File dbPath = new File(targetPath);
		if (dbPath.exists()) {
			deleteDir(dbPath);
		}
		return dbPath;
	}

	private String elasticsearchExecutable() {
		OperatingSystem operatingSystem = this.operatingSystemResolver.currentOperatingSystem();

		switch (operatingSystem.getFamily()) {
		case WINDOWS:
			return ELASTICSEARCH_EXECUTABLE_W;
		default:
			return ELASTICSEARCH_EXECUTABLE_X;
		}

	}

	public void setPort(final int port) {
		this.port = port;
	}

	public void setElasticsearchPath(final String elasticsearchPath) {
		this.elasticsearchPath = elasticsearchPath;
	}

	public void setTargetPath(final String targetPath) {
		this.targetPath = targetPath;
	}

	public void addExtraCommandLineArgument(final String argumentName, final String argumentValue) {
		this.extraCommandArguments.put(argumentName, argumentValue);
	}

	public void addSingleCommandLineArgument(final String argument) {
		this.singleCommandArguments.add(argument);
	}

	public String getElasticsearchPath() {
		return elasticsearchPath;
	}

	protected void setCommandLineExecutor(final CommandLineExecutor commandLineExecutor) {
		this.commandLineExecutor = commandLineExecutor;
	}

	protected void setOperatingSystemResolver(final OperatingSystemResolver operatingSystemResolver) {
		this.operatingSystemResolver = operatingSystemResolver;
	}

	protected void setLowLevelElasticSearchOperations(
			final LowLevelElasticSearchOperations lowLevelElasticSearchOperations) {
		this.lowLevelElasticSearchOperations = lowLevelElasticSearchOperations;
	}

	public class ProcessRunnable implements Runnable {

		private CountDownLatch processIsReady;

		private Process process;

		public ProcessRunnable(final CountDownLatch processIsReady) {
			this.processIsReady = processIsReady;
		}

		@Override
		public void run() {
			try {
				process = startProcess();
			} catch (IOException e) {
				throw prepareException(e);
			} finally {
				processIsReady.countDown();
			}

			try {
				process.waitFor();

			} catch (InterruptedException ie) {
				throw prepareException(ie);
			}

		}

		public void destroyProcess() {
			if (this.process != null) {
				this.process.destroy();
			}
		}

		private IllegalStateException prepareException(final Exception e) {
			return new IllegalStateException("Elasticsearch [" + elasticsearchPath
					+ "] could not be started. Next console message was thrown: " + e.getMessage());
		}

		private Process startProcess() throws IOException {
			return commandLineExecutor.startProcessInDirectoryAndArguments(targetPath,
					buildOperationSystemProgramAndArguments());
		}
	}

	private static String createTempEsFolder() {
		try {
			return Files.createTempDirectory("es").toFile().getAbsolutePath();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
