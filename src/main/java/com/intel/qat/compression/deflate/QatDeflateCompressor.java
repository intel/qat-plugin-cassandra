/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.deflate;

import com.intel.qat.QatZipper;
import io.netty.util.concurrent.FastThreadLocal;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;

/**
 * QatDeflateCompressor is an implementation of ICompressor that uses Intel® QAT (QuickAssist
 * Technology) for hardware-accelerated compression and decompression using Deflate algorithm
 */
public class QatDeflateCompressor implements ICompressor {

  private static final int DEFAULT_RETRY_COUNT = 1000;
  private static final int DEFAULT_COMPRESSION_LEVEL = 1;
  private static final QatZipper.Algorithm QAT_COMPRESSOR_ALGORITHM = QatZipper.Algorithm.DEFLATE;
  private static final QatZipper.Mode QAT_COMPRESSOR_MODE = QatZipper.Mode.AUTO;
  private static final QatZipper.DataFormat QAT_COMPRESSOR_DATAFORMAT = QatZipper.DataFormat.ZLIB;
  private static Map<String, String> compressionOptions;

  private static final FastThreadLocal<QatZipper> qatZipper =
      new FastThreadLocal<QatZipper>() {
        @Override
        protected QatZipper initialValue() {
          QatZipper qzip =
              new QatZipper.Builder()
                  .setAlgorithm(QAT_COMPRESSOR_ALGORITHM)
                  .setLevel(getOrDefaultLevel(compressionOptions))
                  .setMode(QAT_COMPRESSOR_MODE)
                  .setDataFormat(QAT_COMPRESSOR_DATAFORMAT)
                  .setRetryCount(DEFAULT_RETRY_COUNT)
                  .build();
          return qzip;
        }
      };

  /**
   * Creates a QatDeflateCompressor with the given options.
   *
   * @param options A map of configuration options for the compressor.
   * @return An instance of QatDeflateCompressor.
   */
  public static ICompressor create(Map<String, String> options) {
    compressionOptions = options;
    return new QatDeflateCompressor();
  }

  /**
   * Returns the options supported by the compressor. This list should be same as the entries in
   * Cassandra
   *
   * @return A set of the name of options supported
   */
  @Override
  public Set<String> supportedOptions() {
    return Collections.emptySet();
  }

  /**
   * Returns the maximum compression length for the specified source length.
   *
   * @param sourceLen Length of the source buffer being compressed
   * @return the maximum length required for the compressed buffer by Deflate.
   */
  @Override
  public int initialCompressedBufferLength(int sourceLen) {
    return qatZipper.get().maxCompressedLength(sourceLen);
  }

  /**
   * Compresses the source buffer and stores the result in the destination buffer.
   *
   * @param input Buffer holding the source data
   * @param output Buffer holding the compressed data
   * @throws IOException If there is an issue in compressing the data
   */
  @Override
  public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
    int compressedSize;
    try {
      if (input == null || output == null) throw new IOException();
      while (input.hasRemaining()) {
        compressedSize = qatZipper.get().compress(input, output);
        if (compressedSize == 0) {

          // Cleanup threadlocal values
          qatZipper.get().end();
          qatZipper.remove();
          throw new IOException("Output buffer size is small in compress!");
        }
      }
    } catch (IllegalStateException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {

      qatZipper.get().end();
      qatZipper.remove();
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
    int uncompressedSize = 0;
    try {
      if (input == null || output == null) throw new IOException();
      while (input.hasRemaining()) {
        uncompressedSize = qatZipper.get().decompress(input, output);
        if (uncompressedSize == 0) {

          qatZipper.get().end();
          qatZipper.remove();
          throw new IOException("Output buffer size is small in decompress ");
        }
      }
    } catch (IllegalStateException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {

      qatZipper.get().end();
      qatZipper.remove();
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
    int uncompressedSize = 0;
    int inputPosition = inputOffset;
    int inputLimit = inputOffset + inputLength;
    int outputPosition = outputOffset;

    if (inputLength > 0) {
      do {
        try {
          uncompressedSize =
              qatZipper
                  .get()
                  .decompress(
                      input,
                      inputPosition,
                      (inputLimit - inputPosition),
                      output,
                      outputPosition,
                      (output.length - outputPosition));
          if (uncompressedSize == 0) {
            qatZipper.get().end();
            qatZipper.remove();
            throw new IOException("Output buffer size is small in decompress");
          }

          inputPosition += qatZipper.get().getBytesRead();
          outputPosition += uncompressedSize;
        } catch (IllegalStateException
            | IllegalArgumentException
            | ArrayIndexOutOfBoundsException e) {

          qatZipper.get().end();
          qatZipper.remove();
          throw new IOException(e);
        }

      } while (inputPosition < inputLimit);
    }
    return (outputPosition - outputOffset);
  }

  /**
   * Checks if compressor supports provided BufferType
   *
   * @param bufferType BufferType
   * @return true, if supported.
   */
  @Override
  public boolean supports(BufferType bufferType) {
    return true;
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

  // Should be same as that of DeflateCompressor
  private static int getOrDefaultLevel(Map<String, String> options) {
    // No options are supported currently for DeflateCompressor, so return dafault value
    return DEFAULT_COMPRESSION_LEVEL;
  }
}
