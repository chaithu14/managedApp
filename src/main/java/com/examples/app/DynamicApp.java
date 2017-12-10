package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.algo.UniqueCounter;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import static com.datatorrent.api.Context.PortContext.PARTITION_PARALLEL;

@ApplicationAnnotation(name="DynamicDemo")
public class DynamicApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomDynamicGenerator input = dag.addOperator("Input", new RandomDynamicGenerator());
    input.setTuplesBlast(10);
    /*UniqueCounter counter = dag.addOperator("Unique", new UniqueCounter());
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());

    dag.addStream("InputToUniq",input.integer_data,counter.data);
    dag.addStream("UniqToOut", counter.count,output.input);
    dag.setInputPortAttribute(counter.data,PARTITION_PARALLEL,true);*/
    //dag.setInputPortAttribute(output.input,PARTITION_PARALLEL,true);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());
    dag.addStream("InputToOutput",input.integer_data, output.input);
    //dag.setAttribute(output, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<ConsoleOutputOperator>(3));
  }
}
