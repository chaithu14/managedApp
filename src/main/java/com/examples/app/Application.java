package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

@ApplicationAnnotation(name="ManagedStateDemo")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    /*RandomEventGenerator input = dag.addOperator("Input", new RandomEventGenerator());
    input.setTuplesBlast(50);
    input.setMaxvalue(100000);
    ManagedStateIntOperator op = dag.addOperator("State", new ManagedStateIntOperator());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("Input2State", input.integer_data, op.input);
    dag.addStream("ManagedToConsole", op.output, output.input);*/

    long timeInterval = 60000 * 10;
    long bucketTime = 60000 * 1;
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(false);
    input.setMaxTuplesPerWindow(10);
    input.setTuplesPerWindowDeviation(0);
    input.setTimeInterval(timeInterval);
    input.setMaxProductId(1000);
    input.setTimeBucket(bucketTime);

    ManagedStateIntOperator op = dag.addOperator("State", new ManagedStateIntOperator());
    op.setKeyField("productId");
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("Input2State", input.outputPort, op.input);
    dag.addStream("ManagedToConsole", op.output, output.input);

  }
}
