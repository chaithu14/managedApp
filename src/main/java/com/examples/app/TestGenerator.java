package com.examples.app;

import java.util.Date;
import java.util.Random;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class TestGenerator extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<TestEvent> output = new DefaultOutputPort<>();
  private final transient Random r = new Random();

  @Override
  public void emitTuples()
  {
    TestEvent event = new TestEvent();
    event.id = r.nextInt(100);
    output.emit(event);
  }

  public static class TestEvent
  {
    public int id;
    public Date eventTime;

    public TestEvent()
    {
    }

    public int getId()
    {
      return id;
    }

    public void setId(int id)
    {
      this.id = id;
    }

    public Date getEventTime()
    {
      return eventTime;
    }

    public void setEventTime(Date eventTime)
    {
      this.eventTime = eventTime;
    }
  }
}
