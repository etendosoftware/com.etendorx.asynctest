package com.etendorx.asynctest;

import com.etendorx.lib.kafka.model.AsyncProcessState;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class DummyProcess {

  private final SendExecutionStatus sendExecutionStatus;

  public DummyProcess(SendExecutionStatus sendExecutionStatus) {
    this.sendExecutionStatus = sendExecutionStatus;
  }

  @KafkaListener(topicPattern = "topic-test", groupId = "async-test")
  public void listen(String kafkaChange) throws Exception {
    System.out.println("Received data: " + kafkaChange);
    ObjectMapper objectMapper = new ObjectMapper();
    Map<String, Map<String, Object>> data = objectMapper.readValue(kafkaChange,
        new TypeReference<>() {
        });
    String runId = data.get("session").get("run_id").toString();
    SendExecutionStatus.setRunId(runId);
    SendExecutionStatus.setDescription("Processing data " + runId);
    sendExecutionStatus.send("Processing data " + runId, kafkaChange, AsyncProcessState.STARTED);
    Thread.sleep(1000);
    sendExecutionStatus.send("Data processed" + runId, kafkaChange, AsyncProcessState.DONE);
  }
}
