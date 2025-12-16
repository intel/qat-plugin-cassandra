/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.deflate;

import com.intel.qat.QatZipper;
import java.util.Map;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ICompressorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a factory class used to create a Deflate compressor which uses Intel® QAT (QuickAssist
 * Technology)
 */
public class QatDeflateCompressorFactory implements ICompressorFactory {
  private static final Logger logger = LoggerFactory.getLogger(QatDeflateCompressorFactory.class);
  private static final String QAT_NOT_AVAILABLE_MESSAGE = "QAT accelerator is not available.";
  private static final String SUPPORTED_COMPRESSOR_NAME =
      ICompressorFactory.COMPRESSOR_NAME_MAP.get("deflate");

  /** Constructs a new QatDeflateCompressorFactory instance */
  public QatDeflateCompressorFactory() {
    // This will be used by Java ServiceLoader to load the factory which generates a
    // QatDeflateCompressor
  }

  /**
   * @param options Compression options provided by Cassandra
   * @return A compressor object which is an implementation of {@link
   *     org.apache.cassandra.io.compress.ICompressor} interface, which can perform
   *     compress/decompress using QAT hardware, if hardware is available
   * @throws IllegalStateException if hardware is not available, so that Cassandra can use default
   *     compressor
   */
  @Override
  public ICompressor createCompressor(Map<String, String> options) {
    if (QatZipper.isQatAvailable()) {
      logger.info("Loading QAT hardware accelerated compressor..");
      return QatDeflateCompressor.create(options);
    }
    throw new IllegalStateException(QAT_NOT_AVAILABLE_MESSAGE);
  }

  /**
   * Returns compressor class name in Cassandra that the hardware accelerates
   *
   * @return Name of Cassandra compressor class it supports
   */
  @Override
  public String getSupportedCompressorName() {
    return SUPPORTED_COMPRESSOR_NAME;
  }
}
