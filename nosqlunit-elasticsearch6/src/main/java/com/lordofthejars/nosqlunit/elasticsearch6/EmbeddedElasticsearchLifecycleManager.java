package com.lordofthejars.nosqlunit.elasticsearch6;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lordofthejars.nosqlunit.core.AbstractLifecycleManager;

public class EmbeddedElasticsearchLifecycleManager extends AbstractLifecycleManager {
	private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedElasticsearchLifecycleManager.class);
	private static final String LOCALHOST = "127.0.0.1";
	private static final int DEFAULT_PORT = 9300;
	private static final String HOME_PATH_PROPERTY = "path.home";
	private static final String DATA_PATH_PROPERTY = "path.data";

	public static final File EMBEDDED_ELASTICSEARCH_HOME_PATH = createTempEsFolder();
	public static final File EMBEDDED_ELASTICSEARCH_DATA_PATH = new File(EMBEDDED_ELASTICSEARCH_HOME_PATH, "data");

	private File homePath = EMBEDDED_ELASTICSEARCH_HOME_PATH;
	private File dataPath = EMBEDDED_ELASTICSEARCH_DATA_PATH;

	private Settings.Builder settingsBuilder;

	public EmbeddedElasticsearchLifecycleManager() {
		settingsBuilder = Settings.builder().put("node.local", true);
	}

	@Override
	public String getHost() {
		return LOCALHOST + dataPath;
	}

	@Override
	public int getPort() {
		return DEFAULT_PORT;
	}

	@Override
	public void doStart() throws Throwable {
		LOGGER.info("Starting Embedded Elasticsearch instance.");

		settingsBuilder.put(HOME_PATH_PROPERTY, homePath.getAbsolutePath()).put(DATA_PATH_PROPERTY,
				dataPath.getAbsolutePath());
		Node node = elasticsearchNode();
		EmbeddedElasticsearchInstancesFactory.getInstance().addEmbeddedInstance(node, dataPath.getAbsolutePath());

		LOGGER.info("Started Embedded Elasticsearch instance.");
	}

	private Node elasticsearchNode() {
		return new Node(settingsBuilder.build()) {

			@Override
			protected void registerDerivedNodeNameWithLogger(final String nodeName) {

			}
		};
	}

	@Override
	public void doStop() {
		LOGGER.info("Stopping Embedded Elasticsearch instance.");

		Node node = EmbeddedElasticsearchInstancesFactory.getInstance()
				.getEmbeddedByTargetPath(dataPath.getAbsolutePath());

		if (node != null) {
			try {
				node.close();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}

		EmbeddedElasticsearchInstancesFactory.getInstance().removeEmbeddedInstance(dataPath.getAbsolutePath());
		LOGGER.info("Stopped Embedded Elasticsearch instance.");

	}

	public void setSettings(final Settings settings) {
		settingsBuilder.put(settings);
	}

	public void setClient(final boolean client) {
		settingsBuilder.put("node.client", client);
	}

	public void setClusterName(final String clusterName) {
		settingsBuilder.put("cluster.name", clusterName);
	}

	public void setData(final boolean data) {
		settingsBuilder.put("node.data", data);
	}

	public void setLocal(final boolean local) {
		settingsBuilder.put("node.local", local);
	}

	public File getDataPath() {
		return dataPath;
	}

	public void setDataPath(final File dataPath) {
		this.dataPath = dataPath;
	}

	public File getHomePath() {
		return homePath;
	}

	public void setHomePath(final File homePath) {
		this.homePath = homePath;
	}

	private static File createTempEsFolder() {
		try {
			return Files.createTempDirectory("es").toFile();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
