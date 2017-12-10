package com.examples.app;

import java.util.Arrays;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.partitioner.StatelessThroughputBasedPartitioner;

@ApplicationAnnotation(name="FSAPP")
public class FSOutputApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomDynamicGenerator input = dag.addOperator("Input", new RandomDynamicGenerator());
    input.setTuplesBlast(5);
    GenericFileOutputOperator.BytesFileOutputOperator output = dag.addOperator("Output", new GenericFileOutputOperator.BytesFileOutputOperator());
    //BytesNonAppendOutputOperator output = dag.addOperator("Output", new BytesNonAppendOutputOperator());
    output.setFilePath("ftp://chaitanya:test@localhost/home/chaitanya/");
    //output.setFilePath("file:///home/chaitanya/outputDir");
    output.setOutputFileName("ftpoutput.txt");

    dag.addStream("inputToOutput", input.bytedata, output.input);
    //dag.setAttribute(output, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<GenericFileOutputOperator.BytesFileOutputOperator>(2));
    /*StatelessThroughputBasedPartitioner<GenericFileOutputOperator.BytesFileOutputOperator> loadPartitioner = new StatelessThroughputBasedPartitioner<>();
    loadPartitioner.setMaximumEvents(900);
    loadPartitioner.setMinimumEvents(400);
    //loadPartitioner.setCooldownMillis(10);
    dag.setAttribute(output, Context.OperatorContext.STATS_LISTENERS, Arrays.asList(new StatsListener[]{loadPartitioner}));
    dag.setAttribute(output, Context.OperatorContext.PARTITIONER, loadPartitioner);*/
  }
}
