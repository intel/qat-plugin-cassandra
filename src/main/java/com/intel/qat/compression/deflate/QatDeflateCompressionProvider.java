/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.deflate;

import com.intel.qat.QatZipper;
import java.util.Map;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.ICompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a factory class used to create a Deflate compressor which uses Intel® QAT (QuickAssist
 * Technology)
 */
public class QatDeflateCompressionProvider extends AbstractCompressionProvider {
  private static final Logger logger = LoggerFactory.getLogger(QatDeflateCompressionProvider.class);
  private static final String QAT_NOT_AVAILABLE_MESSAGE = "QAT accelerator is not available.";

  /**
   * @param options Compression options provided by Cassandra
   * @return A compressor object which is an implementation of {@link
   *     org.apache.cassandra.io.compress.ICompressor} interface, which can perform
   *     compress/decompress using QAT hardware, if hardware is available
   * @throws IllegalStateException if hardware is not available, so that Cassandra can use default
   *     compressor
   */
  @Override
  public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
      throws IllegalStateException {
    if (QatZipper.isQatAvailable()) {
      logger.info("Loading QAT hardware accelerated compressor..");
      return QatDeflateCompressor.create(options);
    }
    throw new IllegalStateException(QAT_NOT_AVAILABLE_MESSAGE);
  }

  @Override
  public boolean isHealthy()
      throws Exception { // TODO - add checks here to verify if QAT is working properly
    return true;
  }
}
