package net.kotek.jdbm;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings("unchecked")
public class BTreeSetTest extends HTreeSetTest{

    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hs = new BTreeMap(recman, 6, false).keySet();
        Collections.addAll(hs, objArray);
    }
}
