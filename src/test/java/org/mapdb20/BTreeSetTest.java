package org.mapdb10;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

	@Before
    public void setUp() throws Exception {

        hs = new BTreeMap(engine,false,
                BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,null,0),
                6,false,0, BTreeKeySerializer.BASIC,null,
                0).keySet();

        Collections.addAll(hs, objArray);
    }
}
