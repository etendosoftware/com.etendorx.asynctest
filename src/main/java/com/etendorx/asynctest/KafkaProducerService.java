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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * This class is responsible for producing messages to Kafka topics.
 * It is annotated with @Service, meaning it is a Spring managed bean.
 */
@Service
public class KafkaProducerService {

  /**
   * The KafkaTemplate to use for sending messages.
   * This is injected by Spring's dependency injection facilities.
   */
  private final KafkaTemplate<String, String> kafkaTemplate;

  /**
   * Constructs a new KafkaProducerService with the given KafkaTemplate.
   *
   * @param kafkaTemplate the KafkaTemplate to use for sending messages.
   */
  @Autowired
  public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Sends a message to the specified Kafka topic.
   *
   * @param topic the name of the Kafka topic to send the message to.
   * @param runId the run ID to use as the key for the message.
   * @param string the message to send.
   */
  public void sendMessage(String topic, String runId, String string) {
    kafkaTemplate.send(topic, runId, string);
  }

}
