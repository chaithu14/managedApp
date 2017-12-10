package com.examples.app;

import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.apex.malhar.lib.join.POJOInnerJoinOperator;

import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context;
import com.datatorrent.api.Stats;
import com.datatorrent.api.StatsListener;

public class PartitionTestJoinOperator extends POJOInnerJoinOperator implements StatsListener
{
  public int operatorId;
  HashMap<Integer, Integer> partitionMap = Maps.newHashMap();
  transient CountDownLatch latch = new CountDownLatch(1);
  int tuplesProcessed = 0;
  boolean testFailed = false;
  int TOTAL_TUPLES_PROCESS = 1500;
  @AutoMetric
  int tuplesProcessedCompletely = 0;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    operatorId = context.getId();
  }

  @Override
  protected void processTuple(Object tuple, boolean isStream1Data)
  {
    // Verifying the data for stream1
    if (!isStream1Data) {
      return;
    }
    int key = (int)extractKey(tuple, isStream1Data);
    if (partitionMap.containsKey(key)) {
      if (partitionMap.get(key) != operatorId) {
        testFailed = true;
        throw new RuntimeException("Wrong tuple assignment");
      }
    } else {
      partitionMap.put(key, operatorId);
    }
    tuplesProcessed++;
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    tuplesProcessedCompletely = tuplesProcessed;
  }

  @Override
  public StatsListener.Response processStats(StatsListener.BatchedOperatorStats stats)
  {
    Stats.OperatorStats operatorStats = stats.getLastWindowedStats().get(stats.getLastWindowedStats().size() - 1);
    tuplesProcessedCompletely = (Integer)operatorStats.metrics.get("tuplesProcessedCompletely");
    if (tuplesProcessedCompletely >= TOTAL_TUPLES_PROCESS) {
      latch.countDown();
    }
    return null;
  }
}
