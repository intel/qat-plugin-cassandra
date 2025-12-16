/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.deflate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.io.compress.ICompressor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CompressorTests {

  public static Stream<Arguments> provideChunkLengthParams() {
    return Stream.of(
        Arguments.of(0),
        Arguments.of(1),
        Arguments.of(24),
        Arguments.of(153),
        Arguments.of(1000),
        Arguments.of(1024),
        Arguments.of(2048),
        Arguments.of(3071),
        Arguments.of(4096),
        Arguments.of(16384),
        Arguments.of(131072),
        Arguments.of(524288),
        Arguments.of(2097152));
  }

  private ICompressor deflateCompressor;
  private static final Random RANDOM = new Random();

  @BeforeEach
  void setup(TestInfo testInfo) {
    Map<String, String> compressOptions = new HashMap<>();
    deflateCompressor = QatDeflateCompressor.create(compressOptions);
  }

  @AfterEach
  void tearDown(TestInfo testInfo) {
    deflateCompressor = null;
  }

  private byte[] getRandomByteArray(int len) {
    byte[] byteArr = new byte[len];
    if (len > 0) RANDOM.nextBytes(byteArr);
    return byteArr;
  }

  private ByteBuffer getRandomByteBuffer(int len) {
    byte[] byteArr = getRandomByteArray(len);
    ByteBuffer srcBuf = ByteBuffer.allocate(len);
    srcBuf.put(byteArr, 0, len);
    srcBuf.flip();
    return srcBuf;
  }

  private ByteBuffer getRandomDirectByteBuffer(int len) {
    byte[] byteArr = getRandomByteArray(len);
    ByteBuffer srcBuf = ByteBuffer.allocateDirect(len);
    srcBuf.put(byteArr, 0, len);
    srcBuf.flip();
    return srcBuf;
  }

  private ByteBuffer convertStringToByteBuffer(String str) {
    byte[] byteArr = str.getBytes(StandardCharsets.UTF_8);
    return ByteBuffer.wrap(byteArr);
  }

  private ByteBuffer initializePreferredTypeBuffer(BufferType type, int len) {
    return type.allocate(len);
  }

  @Test
  @DisplayName("Tests deflate compress with null source buffer")
  public void testCompressWithNullSourceBuffer() {
    ByteBuffer srcBB = null;
    ByteBuffer compressedBB = ByteBuffer.allocate(10);
    assertThrows(
        IOException.class,
        () -> {
          deflateCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests deflate compress with null destination buffer")
  public void testCompressWithNullCompressedBuffer() {
    ByteBuffer srcBB = ByteBuffer.allocate(10);
    ByteBuffer compressedBB = null;
    assertThrows(
        IOException.class,
        () -> {
          deflateCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests highly compressible data")
  public void testHighlyCompressibleData() {
    try {
      int chunkLength = 16000;
      byte[] srcArray = new byte[chunkLength];
      Arrays.fill(srcArray, (byte) 'Q');
      int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer srcBB = ByteBuffer.allocate(chunkLength);
      srcBB.put(srcArray);
      srcBB.flip();
      ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocate(chunkLength);
      deflateCompressor.compress(srcBB, compressedBB);
      compressedBB.flip();

      deflateCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertArrayEquals(srcBB.array(), resultBB.array());
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @DisplayName("Tests compress/decompress with random sized data")
  @MethodSource("provideChunkLengthParams")
  public void testCompressionByteBufferWithRandomData(int chunkLength) {
    ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
    ByteBuffer resultBB = ByteBuffer.allocate(chunkLength);
    try {
      int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize);

      deflateCompressor.compress(srcBB, compressedBB);
      compressedBB.flip();
      deflateCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertArrayEquals(srcBB.array(), resultBB.array());
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @DisplayName("Tests decompress with byte[] and random sized data")
  @MethodSource("provideChunkLengthParams")
  public void testUncompressWithLengthAndOffset(int chunkLength) {
    try {
      int inOffset = 3;
      int outOffset = 6;
      int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer srcBB = getRandomByteBuffer(chunkLength);

      ByteBuffer compressedBB = ByteBuffer.allocate(compressedSize + inOffset);
      compressedBB.position(inOffset);
      byte[] resultArray = new byte[chunkLength + outOffset];
      deflateCompressor.compress(srcBB, compressedBB);
      compressedBB.flip().position(inOffset);
      deflateCompressor.uncompress(
          compressedBB.array(), inOffset, compressedBB.remaining(), resultArray, outOffset);
      byte[] offsetResultArr = new byte[chunkLength];
      offsetResultArr = Arrays.copyOfRange(resultArray, outOffset, resultArray.length);

      assertArrayEquals(srcBB.array(), offsetResultArr);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  @DisplayName("Tests compress/decompress with different strings")
  void testDifferentStrings() throws IOException {
    String[] testStrings = {
      "Hello World!",
      "Repeated string: " + "Hello World!".repeat(125),
      "Special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?/~`",
      ""
    };

    for (String str : testStrings) {
      int strLength = str.length();
      ByteBuffer srcBB = convertStringToByteBuffer(str);
      int compressedSize = deflateCompressor.initialCompressedBufferLength(strLength);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocate(strLength);
      deflateCompressor.compress(srcBB, compressedBB);
      compressedBB.flip();
      deflateCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertArrayEquals(srcBB.array(), resultBB.array());
    }
  }

  @ParameterizedTest
  @DisplayName("Tests compress/decompress with direct byte buffers and random sized data")
  @MethodSource("provideChunkLengthParams")
  public void testCompressionDirectByteBufferWithRandomData(int chunkLength) {
    int compressedSize = deflateCompressor.initialCompressedBufferLength(chunkLength);
    try {
      ByteBuffer srcBB = getRandomDirectByteBuffer(chunkLength);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
      deflateCompressor.compress(srcBB, compressedBB);
      srcBB.flip();
      compressedBB.flip();
      deflateCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertEquals(resultBB.compareTo(srcBB), 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
