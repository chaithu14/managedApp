package com.examples.app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Operator;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.util.KryoCloneUtils;

public class DynamicProcessOperator implements Operator, Partitioner<DynamicProcessOperator>, StatsListener
{
  private boolean isPartitionRequired = false;
  private boolean repartitiondone = false;
  private Long startWindow;
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
  public final transient DefaultInputPort<String> input1 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      output.emit(s);
    }
  };

  public final transient DefaultInputPort<String> input2 = new DefaultInputPort<String>()
  {
    @Override
    public void process(String s)
    {
      output.emit(s);
    }
  };

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }

  @Override
  public Collection<Partition<DynamicProcessOperator>> definePartitions(Collection<Partition<DynamicProcessOperator>> collection, PartitioningContext partitioningContext)
  {
    boolean isInitialParitition = true;
    // check if it's the initial partition
    if (collection.iterator().hasNext()) {
      isInitialParitition = collection.iterator().next().getStats() == null;
    }
    List<Partition<DynamicProcessOperator>> newPartitions = null;

    if (isInitialParitition) {
      newPartitions = new ArrayList<>();
      newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      //newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      return newPartitions;
    } else if (isPartitionRequired) {
     /*newPartitions = new ArrayList<>();
      for (Partition<RandomDynamicGenerator> g : collection) {
        newPartitions.add(new DefaultPartition<>(g.getPartitionedInstance()));
      }
      newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));*/
      collection.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      repartitiondone = true;
      //return newPartitions;
    }

    return collection;
  }

  @Override
  public void partitioned(Map<Integer, Partition<DynamicProcessOperator>> map)
  {

  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    StatsListener.Response resp = new StatsListener.Response();
    if (startWindow == null) {
      startWindow = batchedOperatorStats.getCurrentWindowId();
    }
    if (startWindow != null && !repartitiondone && batchedOperatorStats.getCurrentWindowId() - startWindow > 150) {
      isPartitionRequired = true;
      resp.repartitionRequired = true;
    }
    System.out.println("WIndowID: " + batchedOperatorStats.getCurrentWindowId());
    return resp;
  }
}
