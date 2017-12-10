package com.examples.app;

import java.io.Serializable;

import com.datatorrent.lib.codec.JavaSerializationStreamCodec;

public class DefaultPartitionCodec extends JavaSerializationStreamCodec<SalesEvent> implements Serializable
{
  /**
   * A codec to enable partitioning to be done by key
   */
  @Override
  public int getPartition(SalesEvent o)
  {
    return o.getProductId();
  }

  private static final long serialVersionUID = 201411031350L;
}
