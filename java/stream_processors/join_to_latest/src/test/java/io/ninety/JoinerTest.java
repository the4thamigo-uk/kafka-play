package io.ninety;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class JoinerTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public JoinerTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( JoinerTest.class );
    }

    public void testApp()
    {
    	final Parser parser = new Schema.Parser();
    	parser.parse("{ \"type\": \"record\", \"name\": \"xrecord\", \"fields\": [ { \"name\": \"event_time\", \"type\": { \"type\": \"long\", \"logicalType\": \"timestamp-millis\" } }, { \"name\": \"key1\", \"type\": \"string\" }, { \"name\": \"key2\", \"type\": \"string\" }, { \"name\": \"val\", \"type\": \"int\" } ] }");
    	
    }
}
