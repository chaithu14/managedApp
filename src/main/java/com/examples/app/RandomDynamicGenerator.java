package com.examples.app;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.StatsListener;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.lib.util.KryoCloneUtils;

public class RandomDynamicGenerator extends RandomEventGenerator implements Partitioner<RandomDynamicGenerator>, StatsListener
{
  private boolean isPartitionRequired = false;
  private boolean repartitiondone = false;
  private Long startWindow;
  public transient DefaultOutputPort<byte[]> bytedata = new DefaultOutputPort<>();
  private final Random random = new Random();

  @Override
  public Collection<Partition<RandomDynamicGenerator>> definePartitions(Collection<Partition<RandomDynamicGenerator>> collection, PartitioningContext partitioningContext)
  {
    boolean isInitialParitition = true;
    // check if it's the initial partition
    if (collection.iterator().hasNext()) {
      isInitialParitition = collection.iterator().next().getStats() == null;
    }
    List<Partition<RandomDynamicGenerator>> newPartitions = null;

    if (isInitialParitition) {
      newPartitions = new ArrayList<>();
      newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      //newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      return newPartitions;
    } else if (isPartitionRequired) {
      for (Partition<RandomDynamicGenerator> g : collection) {
        g.getPartitionedInstance().setTuplesBlast(getTuplesBlast() * 2);
      }
     /*newPartitions = new ArrayList<>();
      for (Partition<RandomDynamicGenerator> g : collection) {
        newPartitions.add(new DefaultPartition<>(g.getPartitionedInstance()));
      }
      newPartitions.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));*/
      //collection.add(new DefaultPartition<>(KryoCloneUtils.cloneObject(this)));
      //repartitiondone = true;
      //return newPartitions;
    }

    return collection;
  }

  @Override
  public void partitioned(Map<Integer, Partition<RandomDynamicGenerator>> map)
  {

  }

  @Override
  public void emitTuples()
  {
    int range = getMaxvalue() - getMinvalue() + 1;
    int i = 0;
    while (i < getTuplesBlast()) {
      int rval = getMinvalue() + random.nextInt(range);
      if (integer_data.isConnected()) {
        integer_data.emit(rval);
      }
      if (string_data.isConnected()) {
        string_data.emit(Integer.toString(rval));
      }
      i++;
      if(bytedata.isConnected()) {
        bytedata.emit(Integer.toString(rval).getBytes());
      }
    }

    if (getTuplesBlastIntervalMillis() > 0) {
      try {
        Thread.sleep(getTuplesBlastIntervalMillis());
      } catch (InterruptedException e) {
        //fixme
      }
    }
  }

  @Override
  public Response processStats(BatchedOperatorStats batchedOperatorStats)
  {
    StatsListener.Response resp = new StatsListener.Response();
    if (startWindow == null) {
      startWindow = batchedOperatorStats.getCurrentWindowId();
    }
    if (startWindow != null && !repartitiondone && batchedOperatorStats.getCurrentWindowId() - startWindow > 100) {
      isPartitionRequired = true;
      //setTuplesBlast(getTuplesBlast() * 2);
      resp.repartitionRequired = true;
    }
    System.out.println("WIndowID: " + batchedOperatorStats.getCurrentWindowId() + " -> " + getTuplesBlast());
    return resp;
  }
}
