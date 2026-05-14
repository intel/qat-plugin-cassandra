/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.zstd;

import com.intel.qat.QatZipper;
import java.util.Map;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.ICompressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a factory class used to create a zstd compressor which uses Intel® QAT (QuickAssist
 * Technology)
 */
public class QatZstdCompressionProvider extends AbstractCompressionProvider {
  private static final Logger logger = LoggerFactory.getLogger(QatZstdCompressionProvider.class);
  private static final String QAT_NOT_AVAILABLE_MESSAGE = "QAT accelerator is not available.";

  /**
   * @param options Compression options provided by Cassandra
   * @return A compressor object which is an implementation of {@link
   *     org.apache.cassandra.io.compress.ICompressor} interface, which can perform
   *     compress/decompress using QAT hardware, if hardware is available
   * @throws IllegalStateException if hardware is not available, so that Cassandra can use default
   *     hardware path
   */
  @Override
  public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
      throws IllegalStateException {
    logger.info("Checking QAT compressor availability");
    if (QatZipper.isQatAvailable()) {
      logger.info("Loading QAT hardware accelerated compressor..");
      return QatZstdCompressor.create(options);
    }
    logger.info("QAT hardware not available..");
    throw new IllegalStateException(QAT_NOT_AVAILABLE_MESSAGE);
  }

  /**
   * Checks if the QAT hardware is available and healthy
   *
   * @return true if QAT is available, false otherwise
   * @throws Exception if there is an error determining the health of the provider
   */
  @Override
  public boolean isHealthy()
      throws Exception { // TODO - add more checks here to verify if QAT is working properly
    boolean healthy = QatZipper.isQatAvailable();
    logger.info("QAT health status : {}", healthy);
    return healthy;
  }
}
