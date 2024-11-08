package com.etendorx.asynctest;

/*
 * Copyright 2022-2024  Futit Services SL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.etendorx.lib.kafka.model.AsyncProcessExecution;
import com.etendorx.lib.kafka.model.AsyncProcessState;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.UUID;

import static com.etendorx.lib.kafka.topology.AsyncProcessTopology.ASYNC_PROCESS_EXECUTION;

/**
 * This class is responsible for sending execution status updates.
 * It uses the KafkaProducerService bean to send the updates.
 * The updates are sent as AsyncProcessExecution messages, which contain information about the execution of an asynchronous process.
 * The class uses ThreadLocal variables to store the run ID and description of the current execution.
 */
@Slf4j
@Component
public class SendExecutionStatus {
  /**
   * KafkaProducerService instance used for sending messages.
   */
  public KafkaProducerService kafkaProducerService;

  /**
   * ThreadLocal variable used to store the run ID of the current execution.
   */
  private static final ThreadLocal<String> runId = new ThreadLocal<>();

  /**
   * ThreadLocal variable used to store the description of the current execution.
   */
  private static final ThreadLocal<String> description = new ThreadLocal<>();

  /**
   * Constructor for the SendExecutionStatus class.
   * Initializes the KafkaProducerService instance.
   *
   * @param kafkaProducerService the KafkaProducerService instance to use for sending messages.
   */
  public SendExecutionStatus(KafkaProducerService kafkaProducerService) {
    this.kafkaProducerService = kafkaProducerService;
  }

  /**
   * This method sets the run ID of the current execution.
   *
   * @param runId the run ID to set.
   */
  public static void setRunId(String runId) {
    SendExecutionStatus.runId.set(runId);
  }

  /**
   * This method sets the description of the current execution.
   *
   * @param description the description to set.
   */
  public static void setDescription(String description) {
    SendExecutionStatus.description.set(description);
  }

  /**
   * This method sends an execution status update.
   * It creates an AsyncProcessExecution message and sends it using the KafkaProducerService.
   * The message contains the log message, parameters, state, run ID, and description of the current execution.
   *
   * @param logMessage the log message to include in the update.
   * @param params     the parameters to include in the update.
   * @param state      the state to include in the update.
   */
  public void send(String logMessage, Object params, AsyncProcessState state) {
    try {
      send(logMessage, new ObjectMapper().writeValueAsString(params), state);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * This method sends an execution status update.
   * It creates an AsyncProcessExecution message and sends it using the KafkaProducerService.
   * The message contains the log message, parameters, state, run ID, and description of the current execution.
   *
   * @param logMessage the log message to include in the update.
   * @param params     the parameters to include in the update.
   * @param state      the state to include in the update.
   */
  public void send(String logMessage, String params, AsyncProcessState state) {
    String runId = SendExecutionStatus.runId.get();
    String description = SendExecutionStatus.description.get();

    AsyncProcessExecution execution = createExecution(runId, logMessage, description, params,
        state);

    try {
      String exec = new ObjectMapper().writeValueAsString(execution);
      kafkaProducerService.sendMessage(ASYNC_PROCESS_EXECUTION, runId, exec);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * This method creates an AsyncProcessExecution object from the provided parameters.
   * It sets the id, asyncProcessId, log, description, params, state, and time of the AsyncProcessExecution to the corresponding input parameters.
   * The params parameter is truncated if its length is more than 1000 characters.
   *
   * @param runId       the run ID to set.
   * @param logMessage  the log message to set.
   * @param description the description to set.
   * @param params      the parameters to set. If the length of params is more than 1000 characters, it is truncated.
   * @param state       the state to set.
   * @return the created AsyncProcessExecution object.
   */
  private AsyncProcessExecution createExecution(String runId, String logMessage, String description,
      String params, AsyncProcessState state) {
    AsyncProcessExecution execution = new AsyncProcessExecution();
    execution.setId(UUID.randomUUID().toString());
    execution.setAsyncProcessId(runId);
    execution.setLog(logMessage);
    execution.setDescription(description);
    execution.setParams(truncateParams(params));
    execution.setState(state);
    execution.setTime(new Date());
    return execution;
  }

  /**
   * This method truncates the input string if its length is more than 1000 characters.
   * If the length of the input string is more than 1000 characters, it returns the first 1000 characters of the string followed by "...".
   * If the length of the input string is 1000 characters or less, it returns the input string as is.
   *
   * @param params the string to truncate.
   * @return the truncated string.
   */
  private String truncateParams(String params) {
    return params.length() > 1000 ? params.substring(0, 1000) + "..." : params;
  }
}

