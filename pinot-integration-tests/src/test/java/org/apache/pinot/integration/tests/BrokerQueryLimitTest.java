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
import com.google.common.collect.ImmutableList;
import java.io.File;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.utils.CommonConstants;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(suiteName = "GeneralClusterIntegrationTest")
// this test uses separate cluster because it needs to change broker configuration
// which is only done once per suite
public class BrokerQueryLimitTest extends SharedClusterIntegrationTest {

  protected static final Logger LOGGER = LoggerFactory.getLogger(BrokerQueryLimitTest.class);
  private static final String LONG_COLUMN = "AirlineID";

  @Test
  public void testWhenLimitIsNOTSetExplicitlyThenDefaultLimitIsApplied()
      throws Exception {
    String query = String.format("SELECT %s FROM %s", LONG_COLUMN, getTableName());

    JsonNode result = postQuery(query).get("resultTable");
    JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

    JsonNode rows = result.get("rows");
    assertEquals(rows.size(), CommonConstants.Broker.DEFAULT_BROKER_QUERY_LIMIT);

    for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
      JsonNode row = rows.get(rowNum);
      assertEquals(row.size(), 1);
    }
  }

  @Test
  public void testWhenLimitISSetExplicitlyThenDefaultLimitIsNotApplied()
      throws Exception {
    String query = String.format("SELECT %s FROM %s limit 20", LONG_COLUMN, getTableName());

    JsonNode result = postQuery(query).get("resultTable");
    JsonNode columnDataTypesNode = result.get("dataSchema").get("columnDataTypes");
    assertEquals(columnDataTypesNode.get(0).textValue(), "LONG");

    JsonNode rows = result.get("rows");
    assertEquals(rows.size(), 20);

    for (int rowNum = 0; rowNum < rows.size(); rowNum++) {
      JsonNode row = rows.get(rowNum);
      assertEquals(row.size(), 1);
    }
  }

}
