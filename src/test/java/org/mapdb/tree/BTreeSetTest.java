package org.mapdb.tree;


import org.junit.Before;
import org.mapdb.DBMaker;

import java.util.Collections;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class BTreeSetTest extends HTreeSetTest {

	@Before
    public void setUp() throws Exception {
        db = DBMaker.memoryDB().make();

        hs = db.treeSet("name").createOrOpen();

        Collections.addAll(hs, objArray);
    }
}
