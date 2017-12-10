package com.examples.app;

import java.io.PrintStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.Stateless;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;

@Stateless
public class ConsolePOJOOutput extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(ConsolePOJOOutput.class);

  private transient PrintStream stream = isStderr() ? System.err : System.out;

  /**
   * This is the input port which receives the tuples that will be written to stdout.
   */
  public final transient DefaultInputPort<SalesEvent> input = new DefaultInputPort<SalesEvent>()
  {
    @Override
    @SuppressWarnings("UseOfSystemOutOrSystemErr")
    public void process(SalesEvent t)
    {
      processEvent(t);
    }

    @Override
    public StreamCodec<SalesEvent> getStreamCodec()
    {
      return new SalesEventCodec();
    }
  };
  public boolean silent = false;

  public class SalesEventCodec extends KryoSerializableStreamCodec<SalesEvent>
  {
    @Override
    public int getPartition(SalesEvent salesEvent)
    {
      return salesEvent.getChannelId();
    }
  }

  public ConsolePOJOOutput()
  {

  }

  public void processEvent(SalesEvent t)
  {
    logger.info("ProcessEvent: {} -> {}",t.customerId % 2, t.hashCode() % 2);
    String s;
    if (stringFormat == null) {
      s = t.toString();
    } else {
      s = String.format(stringFormat, t);
    }
    if (!silent) {
      stream.println(s);
    }
    if (debug) {
      logger.info(s);
    }
  }
  /**
   * @return the silent
   */
  public boolean isSilent()
  {
    return silent;
  }

  /**
   * @param silent the silent to set
   */
  public void setSilent(boolean silent)
  {
    this.silent = silent;
  }

  /**
   * When set to true, tuples are also logged at INFO level.
   */
  private boolean debug;

  /**
   * When set to true, output to stderr
   */
  private boolean stderr = false;
  /**
   * A formatter for {@link String#format}
   */
  private String stringFormat;

  public boolean isDebug()
  {
    return debug;
  }

  public void setDebug(boolean debug)
  {
    this.debug = debug;
  }

  public boolean isStderr()
  {
    return stderr;
  }

  public void setStderr(boolean stderr)
  {
    this.stderr = stderr;
    stream = stderr ? System.err : System.out;
  }

  public String getStringFormat()
  {
    return stringFormat;
  }

  public void setStringFormat(String stringFormat)
  {
    this.stringFormat = stringFormat;
  }
}
