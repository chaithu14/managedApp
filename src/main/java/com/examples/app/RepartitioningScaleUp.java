package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

@ApplicationAnnotation(name="RepartitioningUp")
public class RepartitioningScaleUp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomEventGenerator generator = dag.addOperator("Input", new RandomEventGenerator());
    generator.setTuplesBlast(10);
    DynamicProcessOperator process = dag.addOperator("Process", new DynamicProcessOperator());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("GenToProcess", generator.string_data, process.input1, process.input2);
    dag.addStream("ProcessToOut", process.output, output.input);

  }
}
