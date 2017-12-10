package com.examples.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.io.ConsoleOutputOperator;

@ApplicationAnnotation(name="testjoinApp")
public class TestJoinApp implements StreamingApplication
{
  public PartitionTestJoinOperator joinOp;

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    TestGenerator gen1 = dag.addOperator("Generator1", new TestGenerator());
    TestGenerator gen2 = dag.addOperator("Generator2", new TestGenerator());

    joinOp = dag.addOperator("Join", new PartitionTestJoinOperator());
    joinOp.setLeftKeyExpression("id");
    joinOp.setRightKeyExpression("id");
    joinOp.setIncludeFieldStr("id,eventTime;id,eventTime");
    joinOp.setExpiryTime(10000L);

    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("Gen1ToJoin", gen1.output, joinOp.input1);
    dag.addStream("Gen2ToJoin", gen2.output, joinOp.input2);
    dag.addStream("JoinToConsole", joinOp.outputPort, console.input);
    dag.setInputPortAttribute(joinOp.input1, DAG.InputPortMeta.TUPLE_CLASS,TestGenerator.TestEvent.class);
    dag.setInputPortAttribute(joinOp.input2, DAG.InputPortMeta.TUPLE_CLASS,TestGenerator.TestEvent.class);
    dag.setOutputPortAttribute(joinOp.outputPort, DAG.InputPortMeta.TUPLE_CLASS,TestGenerator.TestEvent.class);
    dag.setAttribute(joinOp, Context.OperatorContext.PARTITIONER,
      new StatelessPartitioner<PartitionTestJoinOperator>(2));
  }
}
