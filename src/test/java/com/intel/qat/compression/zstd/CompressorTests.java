/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.zstd;

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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CompressorTests {
  static Stream<Arguments> provideCompressionLevelParams() {
    return Stream.of(
        Arguments.of(1),
        Arguments.of(2),
        Arguments.of(3),
        Arguments.of(4),
        Arguments.of(5),
        Arguments.of(6),
        Arguments.of(7),
        Arguments.of(8),
        Arguments.of(9));
  }

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
        Arguments.of(64000),
        Arguments.of(131072),
        Arguments.of(524288),
        Arguments.of(2097152));
  }

  private ICompressor zstdCompressor;
  private static final Random RANDOM = new Random();

  @BeforeEach
  void setup() {
    Map<String, String> compressOptions = new HashMap<>();
    zstdCompressor = QatZstdCompressor.create(compressOptions);
  }

  private byte[] getRandomByteArray(int len) {
    byte[] byteArr = new byte[len];
    RANDOM.nextBytes(byteArr);
    return byteArr;
  }

  private ByteBuffer getRandomByteBuffer(int len) {
    byte[] byteArr = getRandomByteArray(len);
    ByteBuffer srcBuf = ByteBuffer.allocateDirect(len);
    srcBuf.put(byteArr, 0, len);
    srcBuf.flip();
    return srcBuf;
  }

  private ByteBuffer convertStringToByteBuffer(String str) {
    byte[] byteArr = str.getBytes(StandardCharsets.UTF_8);
    int len = str.length();
    ByteBuffer srcBuf = ByteBuffer.allocateDirect(len);
    srcBuf.put(byteArr, 0, len);
    srcBuf.flip();
    return srcBuf;
  }

  private ByteBuffer initializePreferredTypeBuffer(BufferType type, int len) {
    return type.allocate(len);
  }

  @Test
  @DisplayName("Tests zstd compress with null source buffer")
  public void testCompressWithNullSourceBuffer() {
    ByteBuffer srcBB = null;
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(10);
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests zstd compress with heap source buffer")
  public void testCompressWithOnHeapSrcBuffer() {
    ByteBuffer srcBB = ByteBuffer.allocate(10);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(10);
    ;
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests zstd compress with heap destination buffer")
  public void testCompressWithOnHeapDestBuffer() {
    ByteBuffer srcBB = ByteBuffer.allocateDirect(10);
    ByteBuffer compressedBB = ByteBuffer.allocate(10);
    ;
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests zstd compress with null destination buffer")
  public void testCompressWithNullCompressedBuffer() {
    ByteBuffer srcBB = ByteBuffer.allocateDirect(10);
    ByteBuffer compressedBB = null;
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests zstd compress with destination buffer  smaller than needed")
  public void testCompressWithCompressBufferOverflow() {
    int chunkLength = 2048;
    int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
    ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize / 2);
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests zstd compress with destination buffer lesser than 512 bytes (minimum needed)")
  public void testWithDesitinationBufferLengthLessThan_512bytes() {
    int chunkLength = 2048;
    ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
    ByteBuffer compressedBB = ByteBuffer.allocateDirect(500);
    assertThrows(
        IOException.class,
        () -> {
          zstdCompressor.compress(srcBB, compressedBB);
        });
  }

  @Test
  @DisplayName("Tests highly compressible data - positive test")
  public void testHighlyCompressibleData() {
    try {
      int chunkLength = 16000;
      byte[] srcArray = new byte[chunkLength];
      Arrays.fill(srcArray, (byte) 'Q');
      int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer srcBB = ByteBuffer.allocateDirect(chunkLength);
      srcBB.put(srcArray, 0, chunkLength);
      srcBB.flip();
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
      zstdCompressor.compress(srcBB, compressedBB);
      compressedBB.flip();
      zstdCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      srcBB.flip();
      assertEquals(resultBB.compareTo(srcBB), 0);
    } catch (IOException e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @DisplayName("Tests compress/decompress with random sized data - positive test")
  @MethodSource("provideChunkLengthParams")
  public void testCompressionByteBufferWithRandomData(int chunkLength) {
    try {
      int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);

      zstdCompressor.compress(srcBB, compressedBB);
      srcBB.flip();
      compressedBB.flip();
      zstdCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertEquals(resultBB.compareTo(srcBB), 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @ParameterizedTest
  @DisplayName("Tests decompress with byte[] and random sized data - positive test")
  @MethodSource("provideChunkLengthParams")
  public void testUncompressionWithLengthAndOffset(int chunkLength) {
    try {
      int inOffset = 3;
      int outOffset = 6;

      ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
      int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
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
      assertEquals(size, chunkLength);
      byte[] offsetResultArr = new byte[chunkLength];
      offsetResultArr = Arrays.copyOfRange(resultArray, outOffset, resultArray.length);

      assertEquals(ByteBuffer.wrap(offsetResultArr).compareTo(srcBB), 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  @Test
  @DisplayName("Tests compress/decompress with different strings - positive test")
  void testDifferentStrings() throws IOException {
    String[] testStrings = {
      "Hello World!",
      "Repeated string: " + "Hello World!".repeat(125),
      "Special characters: !@#$%^&*()_+-=[]{}|;':\",./<>?",
      "" // Empty string
    };

    for (String str : testStrings) {
      int strLength = str.length();
      ByteBuffer srcBB = convertStringToByteBuffer(str);
      int compressedSize = zstdCompressor.initialCompressedBufferLength(strLength);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocateDirect(strLength);
      zstdCompressor.compress(srcBB, compressedBB);
      srcBB.flip();
      compressedBB.flip();
      zstdCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertEquals(resultBB.compareTo(srcBB), 0);
    }
  }

  @ParameterizedTest
  @DisplayName("Tests compress/decompress with different compression levels")
  @MethodSource("provideCompressionLevelParams")
  void TestWithDifferentCompressionLevel(int compressionLevel) throws IOException {
    try {
      int chunkLength = 524288;
      Map<String, String> compressOptions = new HashMap<>();
      compressOptions.put("compression_level", Integer.toString(compressionLevel));
      ICompressor zstdCompressor = QatZstdCompressor.create(compressOptions);
      int compressedSize = zstdCompressor.initialCompressedBufferLength(chunkLength);
      ByteBuffer srcBB = getRandomByteBuffer(chunkLength);
      ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
      ByteBuffer resultBB = ByteBuffer.allocateDirect(chunkLength);
      zstdCompressor.compress(srcBB, compressedBB);
      srcBB.flip();
      compressedBB.flip();
      zstdCompressor.uncompress(compressedBB, resultBB);
      resultBB.flip();
      assertEquals(resultBB.compareTo(srcBB), 0);
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
