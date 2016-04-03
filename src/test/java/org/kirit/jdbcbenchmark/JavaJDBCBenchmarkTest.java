package org.kirit.jdbcbenchmark;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class JavaJDBCBenchmarkTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JavaJDBCBenchmarkTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JavaJDBCBenchmarkTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testJavaJDBCBenchmark()
    {
        assertTrue( true );
    }
}
