package com.examples.app;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

public class processCodec implements Operator
{
  //private static final Logger logger = LoggerFactory.getLogger(processCodec.class);
  private int count = 0;
  SalesEventCodec c ;
  public final transient DefaultOutputPort<SalesEvent> output = new DefaultOutputPort<>();
  public final transient DefaultInputPort<SalesEvent> input = new DefaultInputPort<SalesEvent>()
  {
    @Override
    public void process(SalesEvent tuple)
    {
      //output.emit(tuple);
      processEvent(tuple);
    }

    @Override
    public StreamCodec<SalesEvent> getStreamCodec()
    {
      return getCodec();
    }
  };

  public class SalesEventCodec extends KryoSerializableStreamCodec<SalesEvent> implements Serializable
  {
    public SalesEventCodec()
    {
      super();
    }

    @Override
    public int getPartition(SalesEvent salesEvent)
    {
      return salesEvent.getCustomerId();
    }
  }

  public StreamCodec getCodec()
  {
    return new SalesEventCodec();
  }

  public void processEvent(SalesEvent tuple)
  {
    //logger.info("processEvent: {} -> {}", tuple.customerId % 2, tuple.hashCode() % 2);
    output.emit(tuple);
    count++;
  }

  @Override
  public void beginWindow(long windowId)
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

}
