package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="Kafka8Demo")
public class Kafka8Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    KafkaSinglePortStringInputOperator input = dag.addOperator("Input", new KafkaSinglePortStringInputOperator());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("Input2Output",input.outputPort, output.input);
  }
}
