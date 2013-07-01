package org.mapdb;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

    @Override
	@Before
    public void setUp() throws Exception {
        super.setUp();

        hs = new BTreeMap(engine,BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,null),
                6,false,0, BTreeKeySerializer.BASIC,null,
                Utils.COMPARABLE_COMPARATOR).keySet();

        Collections.addAll(hs, objArray);
    }
}
