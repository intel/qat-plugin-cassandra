/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.zstd;

import com.intel.qat.QatZipper;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;

/**
 * QatZstdCompressor is an implementation of ICompressor that uses Intel® QAT (QuickAssist
 * Technology) for hardware-accelerated compression and decompression using zstd algorithm.
 */
public class QatZstdCompressor implements ICompressor {
  private static final int DEFAULT_RETRY_COUNT = 1000;
  // Compression option names should be same as that used in Cassandra
  private static final String ZSTD_COMPRESSION_LEVEL_NAME = "compression_level";
  private static final int DEFAULT_COMPRESSION_LEVEL = 3;
  private static final QatZipper.Algorithm QAT_COMPRESSOR_ALGORITHM = QatZipper.Algorithm.ZSTD;
  private static final QatZipper.Mode QAT_COMPRESSOR_MODE = QatZipper.Mode.AUTO;
  private static Map<String, String> compressionOptions;

  private static final FastThreadLocal<QatZipper> qatZipper =
      new FastThreadLocal<QatZipper>() {
        @Override
        protected QatZipper initialValue() {
          QatZipper.Builder builder =
              new QatZipper.Builder()
                  .setAlgorithm(QAT_COMPRESSOR_ALGORITHM)
                  .setLevel(getOrDefaultLevel(compressionOptions))
                  .setMode(QAT_COMPRESSOR_MODE)
                  .setRetryCount(DEFAULT_RETRY_COUNT);
          QatZipper qatZipper = builder.build();
          qatZipper.setChecksumFlag(true);
          return qatZipper;
        }
      };

  /**
   * Creates a QatZstdCompressor with the given options.
   *
   * @param options A map of configuration options for the compressor.
   * @return An instance of QatZstdCompressor.
   */
  public static ICompressor create(Map<String, String> options) {
    compressionOptions = options;
    return new QatZstdCompressor();
  }

  /**
   * Returns the options supported by the compressor. This list should be same as the entries in
   * Cassandra
   *
   * @return A set of the name of options supported
   */
  @Override
  public Set<String> supportedOptions() {
    return compressionOptions.keySet();
  }

  /**
   * Returns the maximum compression length for the specified source length.
   *
   * @param sourceLen Length of the source buffer being compressed
   * @return the maximum length required for the compressed buffer by zstd.
   */
  public int initialCompressedBufferLength(int sourceLen) {
    return qatZipper.get().maxCompressedLength(sourceLen);
  }

  /**
   * Compresses the source buffer and stores the result in the destination buffer.
   *
   * @param input Buffer holding the source data
   * @param output Buffer holding the compressed data
   * @throws IOException If there is an issue while compressing the data
   */
  @Override
  public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
    try {
      if (input.hasRemaining()) {
        qatZipper.get().compress(input, output);
      }
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  /**
   * Uncompresses the source buffer and stores the result in the destination buffer.
   *
   * @param input Buffer holding the compressed data
   * @param output Buffer holding the uncompressed data
   * @throws IOException If there is an issue while uncompressing the data
   */
  @Override
  public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
    try {
      if (input.hasRemaining()) {
        qatZipper.get().decompress(input, output);
      }
    } catch (RuntimeException e) {
      throw new IOException(e);
    }
  }

  /**
   * Uncompresses the source byte[] and stores the result in the destination byte[].
   *
   * @param input byte[] holding the compressed data
   * @param inputOffset Offset in the input array to start uncompressing
   * @param inputLength Length of array to uncompress
   * @param output byte[] holding the uncompressed data
   * @param outputOffset Offset in the output array to start writing the uncompressed data
   * @return Size of the uncompressed data in bytes
   * @throws IOException If there is an issue while uncompressing the data
   */
  @Override
  public int uncompress(
      byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
      throws IOException {
    int deCompressedSize = 0;

    try {
      if (inputLength > 0) {
        deCompressedSize =
            qatZipper
                .get()
                .decompress(
                    input,
                    inputOffset,
                    inputLength,
                    output,
                    outputOffset,
                    output.length - outputOffset);
      }
    } catch (RuntimeException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    return deCompressedSize;
  }

  /**
   * Checks if compressor supports provided BufferType
   *
   * @param bufferType BufferType
   * @return true If type is OFF_HEAP, false otherwise.
   */
  @Override
  public boolean supports(BufferType bufferType) {
    return bufferType == BufferType.OFF_HEAP;
  }

  /**
   * Return the BufferType preferred by the compressor
   *
   * @return BufferType Prefered type
   */
  @Override
  public BufferType preferredBufferType() {
    return BufferType.OFF_HEAP;
  }

  // Should be same as that of Cassandra ZstdCompressor
  private static int getOrDefaultLevel(Map<String, String> options) {
    if (options == null) return DEFAULT_COMPRESSION_LEVEL;

    String val = options.get(ZSTD_COMPRESSION_LEVEL_NAME);

    if (val == null) return DEFAULT_COMPRESSION_LEVEL;

    return Integer.valueOf(val);
  }
}
