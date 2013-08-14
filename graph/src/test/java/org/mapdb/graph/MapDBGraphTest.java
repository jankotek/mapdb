package org.mapdb.graph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.impls.GraphTest;

import java.lang.reflect.Method;

public class MapDBGraphTest extends GraphTest {

    public void testGraphTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new GraphTestSuite(this));
        printTestPerformance("GraphTestSuite", this.stopWatch());
    }

    public void testVertexTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new VertexTestSuite(this));
        printTestPerformance("VertexTestSuite", this.stopWatch());
    }

    public void testEdgeTestSuite() throws Exception {
        this.stopWatch();
        doTestSuite(new EdgeTestSuite(this));
        printTestPerformance("EdgeTestSuite", this.stopWatch());
    }

    @Override
    public Graph generateGraph(String s) {
        return generateGraph();
    }

    public Graph generateGraph() {
        String configurationFile = this.getClass().getResource("/graph-test.properties").getPath();
        return GraphFactory.open(configurationFile);
    }

    public void doTestSuite(final TestSuite testSuite) throws Exception {
        String doTest = System.getProperty("testMapDBGraph");
        if (doTest == null || doTest.equals("true")) {
            for (Method method : testSuite.getClass().getDeclaredMethods()) {
                if (method.getName().startsWith("test")) {
                    System.out.println("Testing " + method.getName() + "...");
                    method.invoke(testSuite);
                }
            }
        }
    }

}
