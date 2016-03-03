package org.mapdb;


import org.junit.Before;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest{

	@Before
    public void setUp() throws Exception {
        db = DBMaker.memoryDB().make();

        hs = db.treeSet("name").make();

        Collections.addAll(hs, objArray);
    }
}
