package org.mapdb10;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

	@Before
    public void setUp() throws Exception {

        hs = new BTreeMap(engine,BTreeMap.createRootRef(engine,BTreeKeySerializer.BASIC,null,BTreeMap.COMPARABLE_COMPARATOR,0),
                6,false,0, BTreeKeySerializer.BASIC,null,
                BTreeMap.COMPARABLE_COMPARATOR,0,false).keySet();

        Collections.addAll(hs, objArray);
    }
}
