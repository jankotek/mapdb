package org.mapdb;

public class BTreeMapNavigableSubMapInclusiveTest extends BTreeMapNavigable2Test{

    @Override
	public void setUp() throws Exception {
        super.setUp();
        map.put(0,"zero");
        map.put(11,"eleven");
        map = map.subMap(1,true,10,true);
    }


    @Override
	public void testPut(){
        //this test is not run on submaps
    }
}
