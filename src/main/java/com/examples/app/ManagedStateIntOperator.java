package com.examples.app;

import org.apache.apex.malhar.lib.state.managed.ManagedStateImpl;
import org.apache.commons.lang.ClassUtils;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.datatorrent.lib.fileaccess.FileAccessFSImpl;
import com.datatorrent.lib.util.PojoUtils;
import com.datatorrent.netlet.util.Slice;

public class ManagedStateIntOperator implements Operator, Operator.CheckpointListener, Operator.CheckpointNotificationListener
{
  private transient PojoUtils.Getter keyGetter;
  private String keyField;
  private ManagedStateImpl managedState;
  private transient KryoSerializableStreamCodec streamCodec;
  private long time = System.currentTimeMillis();
  boolean isSearch = false;
  public ManagedStateIntOperator()
  {
    managedState = new ManagedStateImpl();
  }

  @OutputPortFieldAnnotation
  public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

  @InputPortFieldAnnotation
  public transient DefaultInputPort<SalesEvent> input = new DefaultInputPort<SalesEvent>()
  {
    @Override
    public void process(SalesEvent tuple)
    {
      processTuple(tuple);
    }
  };

  void processTuple(SalesEvent tuple)
  {
    if (isSearch) {
      Object value = get(tuple);
      if (value != null) {
        output.emit(value);
      }
    } else {
      put(tuple);
    }
  }

  public Object get(SalesEvent tuple)
  {
    Slice keybytes = streamCodec.toByteArray(keyGetter.get(tuple));
    Slice value = managedState.getSync(0, keybytes);
    if (value != null && value.length != 0) {
      return streamCodec.fromByteArray(value);
    }
    return null;
  }

  public boolean put(Object tuple)
  {
    Slice keybytes = streamCodec.toByteArray(keyGetter.get(tuple));
    Slice valuebytes = streamCodec.toByteArray(tuple);
    managedState.put(0, keybytes, valuebytes);
    return true;
  }

  /*public Object get(Integer key)
  {
    byte[] keybytes = BigInteger.valueOf(key).toByteArray();
    Slice value = managedState.getSync(0, new Slice(keybytes));
    if (value != null && value.length != 0) {
      return new BigInteger(value.buffer).intValue();
    }
    return null;
  }

  public boolean put(Integer tuple)
  {
    managedState.put(0, new Slice(BigInteger.valueOf(tuple).toByteArray()),
      new Slice(BigInteger.valueOf(tuple).toByteArray()));
    return true;
  }*/

  @Override
  public void beginWindow(long windowId)
  {
    managedState.beginWindow(windowId);
    time = System.currentTimeMillis();
  }

  @Override
  public void endWindow()
  {
    managedState.endWindow();
    isSearch = !isSearch;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    streamCodec = new KryoSerializableStreamCodec();
    ((FileAccessFSImpl)managedState.getFileAccess()).setBasePath(context.getValue(DAG.APPLICATION_PATH) + "/" + "bucket_data");
    managedState.setup(context);
    try {
      Class inputClass = SalesEvent.class;
      Class c = ClassUtils.primitiveToWrapper(inputClass.getField(keyField).getType());
      keyGetter = PojoUtils.createGetter(inputClass, keyField, c);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void teardown()
  {
    managedState.teardown();
  }

  @Override
  public void checkpointed(long windowId)
  {
    managedState.checkpointed(windowId);
  }

  @Override
  public void committed(long windowId)
  {
    managedState.committed(windowId);
  }

  @Override
  public void beforeCheckpoint(long windowId)
  {
    managedState.beforeCheckpoint(windowId);
  }

  public String getKeyField()
  {
    return keyField;
  }

  public void setKeyField(String keyField)
  {
    this.keyField = keyField;
  }
}
