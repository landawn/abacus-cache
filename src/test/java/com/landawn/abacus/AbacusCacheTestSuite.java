package com.landawn.abacus;

import org.junit.platform.suite.api.IncludeClassNamePatterns;
import org.junit.platform.suite.api.IncludeEngines;
import org.junit.platform.suite.api.SelectPackages;
import org.junit.platform.suite.api.Suite;

/**
 * Optional unit-test launcher for IDEs and {@code mvn -Dtest=AbacusCacheTestSuite test}.
 * Selects the same {@code *Test} classes as Maven; integration and performance tests use separate profiles.
 * See {@code docs/TESTING.md} for the VM options required by direct IDE runs.
 */
@Suite
@SelectPackages("com.landawn.abacus")
@IncludeClassNamePatterns(".*Test")
@IncludeEngines("junit-jupiter")
public class AbacusCacheTestSuite {
}
