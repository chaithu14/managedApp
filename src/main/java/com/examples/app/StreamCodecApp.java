package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="streamCodecApp")
public class StreamCodecApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", new JsonSalesGenerator());
    processCodec process = dag.addOperator("Process", processCodec.class);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("InputToprocess", input.outputPort, process.input);
    dag.addStream("processToO", process.output, output.input);
    dag.setAttribute(process, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<processCodec>(2));
  }
}
