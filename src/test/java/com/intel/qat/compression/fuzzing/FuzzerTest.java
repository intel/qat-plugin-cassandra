/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.fuzzing;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.intel.qat.compression.deflate.QatDeflateCompressor;
import com.intel.qat.compression.zstd.QatZstdCompressor;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.cassandra.io.compress.ICompressor;

public class FuzzerTest {
  private static final Random RANDOM = new Random();
  private static final int minVal = -7, maxVal = 22; // zstd compression levels to choose from

  public static void fuzzerTestOneInput(FuzzedDataProvider data) {
    try {
      if (data.remainingBytes() == 0) return;
      byte[] src = data.consumeRemainingAsBytes();
      fuzzDeflateHeapByteBuffers(src);
      fuzzDeflateDirectByteBuffers(src);
      fuzzDeflateDirectSrcByteBuffer(src);
      fuzzDeflateDirectDestByteBuffer(src);
      fuzzDeflateUncompressWithLengthAndOffset(src);

      fuzzZstdDirectByteBuffers(src);
      fuzzZstdByteBuffersWithCompressionLevel(src);
      fuzzZstdUncompressWithLengthAndOffset(src);
    } catch (IOException e) {
      System.out.println("Unexpected exception: " + e.getMessage());
    }
  }

  private static void fuzzDeflateHeapByteBuffers(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocate(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor deflateCompressor = QatDeflateCompressor.create(compressOptions);
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocate(chunkLength);
    deflateCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    deflateCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzDeflateDirectByteBuffers(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor deflateCompressor = QatDeflateCompressor.create(compressOptions);
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
    deflateCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    deflateCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzDeflateDirectSrcByteBuffer(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor deflateCompressor = QatDeflateCompressor.create(compressOptions);
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocate(chunkLength);
    deflateCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    deflateCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzDeflateDirectDestByteBuffer(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocate(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor deflateCompressor = QatDeflateCompressor.create(compressOptions);
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
    deflateCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    deflateCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzDeflateUncompressWithLengthAndOffset(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocate(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor deflateCompressor = QatDeflateCompressor.create(compressOptions);
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    int inOffset = RANDOM.nextInt(chunkLength);
    int outOffset = RANDOM.nextInt(compressedSize);

    ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize + inOffset);
    compressedBB.position(inOffset);
    deflateCompressor.compress(srcBB, compressedBB);
    compressedBB.flip().position(inOffset);
    byte[] resultArray = new byte[chunkLength + outOffset];
    deflateCompressor.uncompress(
        compressedBB.array(), inOffset, compressedBB.remaining(), resultArray, outOffset);
    byte[] offsetResultArr = new byte[chunkLength];
    offsetResultArr = Arrays.copyOfRange(resultArray, outOffset, resultArray.length);
    assert Arrays.equals(srcBB.array(), offsetResultArr)
        : "The source and decompressed buffers do not match.";
  }

  private static void fuzzZstdDirectByteBuffers(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor zstdCompressor = QatZstdCompressor.create(compressOptions);
    int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
    zstdCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    zstdCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzZstdByteBuffersWithCompressionLevel(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    int compressionLevel = RANDOM.nextInt(maxVal - minVal) + minVal;
    Map<String, String> compressOptions = new HashMap<>();
    compressOptions.put("compression_level", Integer.toString(compressionLevel));
    ICompressor zstdCompressor = QatZstdCompressor.create(compressOptions);
    int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
    ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
    zstdCompressor.compress(srcBB, compressedBB);
    srcBB.flip();
    compressedBB.flip();
    zstdCompressor.uncompress(compressedBB, resultBB);
    resultBB.flip();
    assert srcBB.compareTo(resultBB) == 0 : "The source and decompressed buffers do not match.";
  }

  private static void fuzzZstdUncompressWithLengthAndOffset(byte[] byteArr) throws IOException {
    int chunkLength = byteArr.length;
    Map<String, String> compressOptions = new HashMap<>();
    ICompressor zstdCompressor = QatZstdCompressor.create(compressOptions);

    ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
    srcBB.put(byteArr, 0, chunkLength);
    srcBB.flip();
    int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
    int inOffset = RANDOM.nextInt(chunkLength);
    int outOffset = RANDOM.nextInt(compressedSize);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize + inOffset);
    compressedBB.position(inOffset);
    byte[] resultArray = new byte[chunkLength + outOffset];
    zstdCompressor.compress(srcBB, compressedBB);

    compressedBB.flip().position(inOffset);
    srcBB.flip();

    int compressed_size = compressedBB.remaining();
    byte[] compressedByteArray = new byte[compressedBB.limit()];
    compressedBB.get(compressedByteArray, inOffset, compressedBB.remaining());
    int size =
        zstdCompressor.uncompress(
            compressedByteArray, inOffset, compressed_size, resultArray, outOffset);

    byte[] offsetResultArr = new byte[size];
    offsetResultArr = Arrays.copyOfRange(resultArray, outOffset, resultArray.length);
    assert srcBB.compareTo(ByteBuffer.wrap(offsetResultArr)) == 0
        : "The source and decompressed buffers do not match.";
  }
}
