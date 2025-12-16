/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.zstd;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.ICompressorFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CompressorFactoryTests {
  ServiceLoader<ICompressorFactory> loader;
  private QatZstdCompressorFactory factory;

  @BeforeEach
  void setup() {
    loader = ServiceLoader.load(ICompressorFactory.class);
    factory = new QatZstdCompressorFactory();
  }

  public void testZstdFactoryName() {}

  @Test
  public void testServiceLoaderTest() {
    loader = ServiceLoader.load(ICompressorFactory.class);
    Map<String, String> options = new HashMap<>();
    Optional<ICompressorFactory> compressorFactory =
        loader.stream()
            .filter(
                provider -> provider.get().getSupportedCompressorName().equals("ZstdCompressor"))
            .map(ServiceLoader.Provider::get)
            .findFirst();

    assertTrue(compressorFactory.isPresent());
    ICompressor compressor = compressorFactory.get().createCompressor(options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  public void testServiceLoaderTestNegative() {
    loader = ServiceLoader.load(ICompressorFactory.class);
    Optional<ICompressorFactory> compressorFactory =
        loader.stream()
            .filter(provider -> provider.get().getSupportedCompressorName().equals("TestString"))
            .map(ServiceLoader.Provider::get)
            .findFirst();
    assertFalse(compressorFactory.isPresent());
  }

  @Test
  public void testCreateCompressorWithNullOptions() {
    ICompressor compressor = factory.createCompressor(null);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  public void testCreateCompressorWithEmptyOptions() {
    Map<String, String> options = new HashMap<>();
    ICompressor compressor = factory.createCompressor(options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  @DisplayName("Tests factory creation with valid compression level")
  public void testCreateCompressorWithValidCompressionLevel() {
    Map<String, String> options = new HashMap<>();
    options.put("compression_level", "6");
    ICompressor compressor = factory.createCompressor(options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  @DisplayName("Tests factory creation with invalid compression level")
  public void testCreateCompressorWithInvalidCompressionLevel() {
    Map<String, String> options = new HashMap<>();
    options.put("compression_level", "-1");
    ICompressor compressor = factory.createCompressor(options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  @DisplayName("Tests compress operation after creating compressor using factory")
  public void testCreateCompressorAndCompress() {
    byte[] srcArray = "Hello, World!".getBytes(StandardCharsets.UTF_8);
    ByteBuffer srcBB = ByteBuffer.allocateDirect(srcArray.length);
    srcBB.put(srcArray, 0, srcArray.length);
    srcBB.flip();
    Map<String, String> options = new HashMap<>();
    options.put("compression_level", "6");
    ICompressor compressor = factory.createCompressor(options);
    assertNotNull(compressor);
    assertDoesNotThrow(
        () -> {
          int compressedSize = compressor.initialCompressedBufferLength(srcArray.length);
          ByteBuffer compressedBB = ByteBuffer.allocateDirect(compressedSize);
          ByteBuffer resultBB = ByteBuffer.allocateDirect(srcArray.length);
          compressor.compress(srcBB, compressedBB);
          compressedBB.flip();
          compressor.uncompress(compressedBB, resultBB);
        });
  }
}
