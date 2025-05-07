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
package org.apache.pinot.controller.validation.tableconfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TableConfigValidatorRegistry {
  
  private static final Logger LOGGER = LoggerFactory.getLogger(TableConfigValidatorRegistry.class);
  private static final List<TableConfigValidator> VALIDATORS = new ArrayList<>();
  private static boolean _init = false;
  
  private TableConfigValidatorRegistry() {
  }
  
  /**
   * Initialize validators from a list of class names
   * @param validatorClassNames List of class names for validators
   */
  public synchronized static void init(List<String> validatorClassNames, PinotHelixResourceManager resourceManager) {
    if (_init) {
      LOGGER.info("TableConfigValidatorRegistry already initialized, skipping.",
          new Throwable("TableConfigValidatorRegistry already initialized"));
      return;
    }

    for (String className : validatorClassNames) {
      try {
        Class<?> validatorClass = Class.forName(className);
        if (TableConfigValidator.class.isAssignableFrom(validatorClass)) {
          TableConfigValidator validator = (TableConfigValidator)
              validatorClass.getDeclaredConstructor(PinotHelixResourceManager.class).newInstance(resourceManager);
          VALIDATORS.add(validator);
          LOGGER.info("Added table config validator: {}", className);
        } else {
          LOGGER.error("Class {} does not implement TableConfigValidator interface, skipping", className);
        }
      } catch (ClassNotFoundException e) {
        LOGGER.error("Could not find class: {}", className, e);
      } catch (ReflectiveOperationException e) {
        LOGGER.error("Could not instantiate class: {}", className, e);
      }
    }

    if (VALIDATORS.isEmpty()) {
      LOGGER.info("No table config validators found");
    }

    _init = true;
  }

  public static List<TableConfigValidator> getValidators() {
    if (!_init) {
      throw new IllegalStateException("TableConfigValidatorRegistry is not initialized. Call init() first.");
    }
    return Collections.unmodifiableList(VALIDATORS);
  }

}
