package org.apache.pinot.integration.tests;

import java.io.IOException;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.util.TestUtils;

/*
  * Manage all components required for an integration test - broker, server, minion, helix, zk, controller, kafka, etc
 */
public class SharedCluster extends BaseClusterIntegrationTest {

  void createDataDirs() throws IOException {
    TestUtils.ensureDirectoriesExistAndEmpty(_tempDir, _segmentDir, _tarDir);
  }

  void startCluster() throws Exception {
    startZk();
    startKafka();
    startController();
    startHelix();
    startBroker();
    startServer();
  }

  void stopCluster() {
    stopKafka();
    stopServer();
    stopBroker();
    stopController();
    stopZk();
  }

  private void startHelix() {
    HelixConfigScope scope =
        new HelixConfigScopeBuilder(HelixConfigScope.ConfigScopeProperty.CLUSTER).forCluster(getHelixClusterName())
            .build();

    // TODO - Take PinotConfig as an arg and use that
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_STARTREE_PREPROCESS_PARALLELISM, Integer.toString(8));
    _helixManager.getConfigAccessor()
        .set(scope, CommonConstants.Helix.CONFIG_OF_MAX_SEGMENT_DOWNLOAD_PARALLELISM, Integer.toString(8));
  }

}
