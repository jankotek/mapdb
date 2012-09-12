package net.kotek.jdbm;


import org.junit.Before;

public class BTreeSetTest extends HTreeSetTest{

    /**
     * Sets up the fixture, for example, open a network connection. This method
     * is called before a test is executed.
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        hs = new BTreeMap(recman, 6, false).keySet();
        for (int i = 0; i < objArray.length; i++)
            hs.add(objArray[i]);
    }
}
