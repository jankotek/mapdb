package org.mapdb;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();
        hs = new BTreeMap(engine, 6, false,false,false, null,null,null,null).keySet();
        Collections.addAll(hs, objArray);
    }
}
