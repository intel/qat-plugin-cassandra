/*
 * Copyright (c) 2025 Intel Corporation
 *
 * SPDX-License-Identifier: MIT
 */

package com.intel.qat.compression;

import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

@Suite
// @SelectPackages({"com.intel.qat.plugin.cassandra.deflate"})
@SelectPackages({"com.intel.qat.compression.deflate", "com.intel.qat.compression.zstd"})
public class QatPluginTestSuite {}
