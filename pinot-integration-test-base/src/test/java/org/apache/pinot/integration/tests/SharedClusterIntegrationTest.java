/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.integration.tests;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.model.builder.HelixConfigScopeBuilder;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Parameters;


/*
  * Base class for integration tests that use a shared Pinot cluster.
  * A shared cluster across tests has an advantage of avoiding the overhead of starting and stopping the cluster for each test.
  * Additionally, a shared cluster helps to run tests parallely because starting multiple clusters parallely can be resource intensive and breaks assumptions in Pinot code (eg: ControllerLeaderLocator is a singleton).
  *
  * A cluster can be shared through different schemas / tables / tenants
  * Test cases control how isolated they are from each other.
  *
  * Tests leveraging a shared cluster are defined in a test suite (defined in pinot-integration-tests/src/test/resources).
  * Each test suite can be configured based on its needs (eg: dataset, tables, parallelism (classes vs methods vs serial), etc)
  * There can be multiple test suites each with its own shared cluster. Each test suite will run serially.
 */
public abstract class SharedClusterIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(SharedClusterIntegrationTest.class);

  // Don't want to expose sharedCluster outside this class as it will hard to contain what tests do with it (eg: disable multi stage engine, etc).
  // Thus all operations on shared cluster would be through this class which will make it easier to limit what can be done.
  private static SharedCluster sharedCluster; // static so that it can be shared across test class implementations

  private static String tableName;

  private static final Long TIMEOUT_TO_LOAD_DATA = 60_000L;

  @Parameters({"schemaFileName", "tableName", "avroTarFileName", "numReplicas"})
  @BeforeSuite
  public void setUpSuite(String schemaFileName, String tableName, String avroTarFileName, int numReplicas)
      throws Exception {
    LOGGER.info("Setting up integration test suite");

    // Re-initialised whenever a new test suite is being executed
    sharedCluster = new SharedCluster();
    SharedClusterIntegrationTest.tableName = tableName;

    sharedCluster.createDataDirs();
    sharedCluster.startCluster();

    // Create and upload the schema and table config
    Schema schema = addSchema(schemaFileName);
    TableConfig tableConfig = addTable(tableName, numReplicas);
    createAndUploadSegments(avroTarFileName, schema, tableConfig);

    LOGGER.info("Finished setting up integration test suite");
  }

  @Parameters({"schemaFileName", "tableName", "avroTarFileName", "numReplicas"})
  @AfterSuite
  public void tearDownSuite(String schemaFileName, String tableName, String avroTarFileName, int numReplicas)
      throws Exception {
    sharedCluster.dropOfflineTable(tableName);
    sharedCluster.stopCluster();
  }

  protected JsonNode postQuery(String query)
      throws Exception {
    return sharedCluster.postQuery(query);
  }

  protected String getTableName() {
    return tableName;
  }

  private Schema addSchema(String schemaFileName)
      throws IOException {
    Schema schema = sharedCluster.createSchema(schemaFileName);
    sharedCluster.addSchema(schema);
    return schema;
  }

  private TableConfig addTable(String tableName, int numReplicas)
      throws IOException {
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName)
        .setNumReplicas(numReplicas).build();
    tableConfig.getIndexingConfig().setStarTreeIndexConfigs(new ArrayList<>());
    tableConfig.getIndexingConfig().setEnableDynamicStarTreeCreation(true);
    sharedCluster.addTableConfig(tableConfig);
    return tableConfig;
  }

  private void createAndUploadSegments(String avroTarFileName, Schema schema, TableConfig tableConfig)
      throws Exception {
    List<File> avroFiles = sharedCluster.unpackTarData(avroTarFileName, sharedCluster._tempDir);
    ClusterIntegrationTestUtils.buildSegmentsFromAvro(avroFiles, tableConfig, schema, 0, sharedCluster._segmentDir, sharedCluster._tarDir);
    sharedCluster.uploadSegments(tableConfig.getTableName(), sharedCluster._tarDir);
    sharedCluster.waitForAllDocsLoaded(TIMEOUT_TO_LOAD_DATA);
  }
}
