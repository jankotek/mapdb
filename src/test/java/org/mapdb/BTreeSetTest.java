package org.mapdb;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings("unchecked")
public class BTreeSetTest extends HTreeSetTest{

    @Before
    public void setUp() throws Exception {
        super.setUp();
        hs = new BTreeMap(recman, 6, false,null,null,null,null).keySet();
        Collections.addAll(hs, objArray);
    }
}
