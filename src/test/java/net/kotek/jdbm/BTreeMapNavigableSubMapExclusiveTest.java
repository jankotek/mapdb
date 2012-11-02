package net.kotek.jdbm;


import org.junit.Ignore;

@Ignore
public class BTreeMapNavigableSubMapExclusiveTest extends BTreeMapNavigable2Test{

    public void setUp() throws Exception {
        super.setUp();
        map.put(-1,"-one");
        map.put(0,"zero");
        map.put(11,"eleven");
        map.put(12,"twelve");
        map = map.subMap(0,false,11,false);
    }


    public void testPut(){
        //this test is not run on submaps
    }
}
