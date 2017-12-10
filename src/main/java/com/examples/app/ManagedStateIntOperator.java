package com.examples.app;

import java.lang.reflect.Array;
import java.util.Calendar;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.managed.ManagedTimeStateImpl;
import org.apache.commons.lang.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateIntOperator implements Operator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  private static final transient Logger LOG = LoggerFactory.getLogger(ManagedStateIntOperator.class);
  private transient PojoUtils.Getter[] keyGetter = (PojoUtils.Getter[])Array.newInstance(PojoUtils.Getter.class, 2);
  private String keyField;
  private ManagedTimeStateImpl streamData1;
  //private ManagedTimeStateImpl streamData2;
  private transient KryoSerializableStreamCodec streamCodec;
  private long time = System.currentTimeMillis();
  private Long bucketSpan;
  private Long expiryBefore;
  private boolean isLeft;
  public ManagedStateIntOperator()
  {
    LOG.info("Constructor: {}", keyField);
    streamData1 = new ManagedTimeStateImpl();
    //streamData2 = new ManagedTimeStateImpl();
    isLeft = false;
  }

  @OutputPortFieldAnnotation
  public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @InputPortFieldAnnotation
  public transient DefaultInputPort<SalesEvent> input = new DefaultInputPort<SalesEvent>()
  {
    @Override
    public void process(SalesEvent tuple)
    {
      isLeft = true;
      processTuple(tuple);
    }

    @Override
    public StreamCodec<SalesEvent> getStreamCodec()
    {
      return new DefaultPartitionCodec();
    }
  };

  @InputPortFieldAnnotation
  public transient DefaultInputPort<ProductEvent> product = new DefaultInputPort<ProductEvent>()
  {
    @Override
    public void process(ProductEvent productEvent)
    {
      isLeft = false;
      processTuple(productEvent);
    }
  };

  void processTuple(Object tuple)
  {
    put(tuple);
    Object value = get(tuple);
    if(value != null) {
      output.emit(value);
    }
  }

  public Object get(Object tuple)
  {
    PojoUtils.Getter getter = isLeft ? keyGetter[0] : keyGetter[1];
    ManagedTimeStateImpl managedState = isLeft ? streamData1 : streamData1;
    Slice keybytes = streamCodec.toByteArray(getter.get(tuple));
    Slice value = managedState.getSync(0, keybytes);
    if (value != null && value.length != 0) {
      return streamCodec.fromByteArray(value);
    }
    return null;
  }

  public boolean put(Object tuple)
  {
    PojoUtils.Getter getter = isLeft ? keyGetter[0] : keyGetter[1];
    ManagedTimeStateImpl managedState = isLeft ? streamData1 : streamData1;
    Slice keybytes = streamCodec.toByteArray(getter.get(tuple));
    Slice valuebytes = streamCodec.toByteArray(tuple);
    managedState.put(0, Calendar.getInstance().getTimeInMillis(), keybytes, valuebytes);
    return true;
  }

  @Override
  public void beginWindow(long windowId)
  {
    streamData1.beginWindow(windowId);
    //streamData2.beginWindow(windowId);
    time = System.currentTimeMillis();
  }

  @Override
  public void endWindow()
  {
    streamData1.endWindow();
    //streamData2.endWindow();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    //streamData1.getCheckpointManager().setRecoveryPath("stream1/managed_state");
    streamCodec = new KryoSerializableStreamCodec();
    ((FileAccessFSImpl)streamData1.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data_" + 0);
    //((FileAccessFSImpl)streamData2.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data_" + 1);
    if (bucketSpan != null) {
      streamData1.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpan));
      //streamData2.getTimeBucketAssigner().setBucketSpan(Duration.millis(bucketSpan));
    }
    if (expiryBefore != null) {
      streamData1.getTimeBucketAssigner().setExpireBefore(Duration.millis(expiryBefore));
      //streamData2.getTimeBucketAssigner().setExpireBefore(Duration.millis(expiryBefore));
    }
    streamData1.setup(context);
    //streamData2.setup(context);
    try {
      Class inputClass = SalesEvent.class;
      Class c = ClassUtils.primitiveToWrapper(inputClass.getField(keyField).getType());
      keyGetter[0] = PojoUtils.createGetter(inputClass, keyField, c);
      Class inputClass1 = ProductEvent.class;
      Class c1 = ClassUtils.primitiveToWrapper(inputClass1.getField(keyField).getType());
      keyGetter[1] = PojoUtils.createGetter(inputClass1, keyField, c1);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    streamData1.teardown();
    //streamData2.teardown();
  }

  @Override
  public void checkpointed(long windowId)
  {
    streamData1.checkpointed(windowId);
    //streamData2.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    streamData1.committed(windowId);
    //streamData2.committed(windowId);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    streamData1.beforeCheckpoint(windowId);
    //streamData2.beforeCheckpoint(windowId);
  }

  public String getKeyField()
  {
    return keyField;
  }

  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }

  public Long getBucketSpan()
  {
    return bucketSpan;
  }

  public void setBucketSpan(Long bucketSpan)
  {
    this.bucketSpan = bucketSpan;
  }

  public Long getExpiryBefore()
  {
    return expiryBefore;
  }

  public void setExpiryBefore(Long expiryBefore)
  {
    this.expiryBefore = expiryBefore;
  }
}
