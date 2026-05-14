/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression.zstd;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import org.apache.cassandra.io.compress.AbstractCompressionProvider;
import org.apache.cassandra.io.compress.ICompressor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class CompressorFactoryTests {
  ServiceLoader<AbstractCompressionProvider> loader;
  private QatZstdCompressionProvider provider;

  @BeforeEach
  void setup() {
    loader = ServiceLoader.load(AbstractCompressionProvider.class);
    provider = new QatZstdCompressionProvider();
  }

  @Test
  public void testServiceLoaderTest() {
    Map<String, String> options = new HashMap<>();
    Optional<AbstractCompressionProvider> compressorFactory =
        loader.stream()
            .filter(
                provider ->
                    provider.get().getClass().getSimpleName().equals("QatZstdCompressionProvider"))
            .map(ServiceLoader.Provider::get)
            .findFirst();

    assertTrue(compressorFactory.isPresent());
    ICompressor compressor =
        compressorFactory.get().createCompressor(QatZstdCompressor.class, options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  public void testServiceLoaderTestNegative() {
    Optional<AbstractCompressionProvider> compressorFactory =
        loader.stream()
            .filter(provider -> provider.get().getClass().getSimpleName().equals("TestString"))
            .map(ServiceLoader.Provider::get)
            .findFirst();
    assertFalse(compressorFactory.isPresent());
  }

  @Test
  public void testCreateCompressorWithNullOptions() {
    ICompressor compressor = provider.createCompressor(QatZstdCompressor.class, null);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  public void testCreateCompressorWithEmptyOptions() {
    Map<String, String> options = new HashMap<>();
    ICompressor compressor = provider.createCompressor(QatZstdCompressor.class, options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  @DisplayName("Tests factory creation with valid compression level")
  public void testCreateCompressorWithValidCompressionLevel() {
    Map<String, String> options = new HashMap<>();
    options.put("compression_level", "6");
    ICompressor compressor = provider.createCompressor(QatZstdCompressor.class, options);
    assertNotNull(compressor);
    assertInstanceOf(QatZstdCompressor.class, compressor);
  }

  @Test
  @DisplayName("Tests factory creation with negative compression level")
  public void testCreateCompressorWithNegativeCompressionLevel() {
    Map<String, String> options = new HashMap<>();
    options.put("compression_level", "-1");
    ICompressor compressor = provider.createCompressor(QatZstdCompressor.class, options);
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
    ICompressor compressor = provider.createCompressor(QatZstdCompressor.class, options);
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
