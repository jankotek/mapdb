package org.mapdb;

import org.junit.Test;
import java.util.*;
import static org.mapdb.Fun.*;
import static org.junit.Assert.*;

public class BindTest {

    BTreeMap<Integer,String> m = DBMaker.newMemoryDB().make().getTreeMap("test");

    String[] split(String s){
        if(s==null) return null;
        String[] ret = new String[s.length()];
        for(int i=0;i<ret.length;i++)
            ret[i] = ""+s.charAt(i);
        return ret;
    }

    @Test public void secondary_values(){
        m.put(1,"jedna");
        m.put(2,"dve");

        Set<Tuple2<Integer,String>> sec = new TreeSet<Tuple2<Integer, String>>();

        Bind.secondaryValues(m,sec,new Function2<String[], Integer, String>() {
            @Override
            public String[] run(Integer integer, String s) {
                return split(s);
            }
        });

        //filled if empty
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2(2,"d")));
        assert(sec.contains(t2(2,"v")));
        assert(sec.contains(t2(2,"e")));

        //old values preserved
        m.put(2,"dvea");
        assertEquals(5+4,sec.size());
        assert(sec.contains(t2(2,"d")));
        assert(sec.contains(t2(2,"v")));
        assert(sec.contains(t2(2,"e")));
        assert(sec.contains(t2(2,"a")));

        //old values deleted
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2(2,"d")));
        assert(sec.contains(t2(2,"v")));
        assert(sec.contains(t2(2,"a")));

        //all removed on delete
        m.remove(2);
        assertEquals(5,sec.size());

        //all added on put
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2(2,"d")));
        assert(sec.contains(t2(2,"v")));
        assert(sec.contains(t2(2,"a")));

    }

    @Test public void secondary_keys(){
        m.put(1,"jedna");
        m.put(2,"dve");

        Set<Tuple2<String,Integer>> sec = new TreeSet<Tuple2<String,Integer>>();

        Bind.secondaryKeys(m,sec,new Function2<String[], Integer, String>() {
            @Override
            public String[] run(Integer integer, String s) {
                return split(s);
            }
        });

        //filled if empty
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2("d",2)));
        assert(sec.contains(t2("v",2)));
        assert(sec.contains(t2("e",2)));

        //old values preserved
        m.put(2,"dvea");
        assertEquals(5+4,sec.size());
        assert(sec.contains(t2("d",2)));
        assert(sec.contains(t2("v",2)));
        assert(sec.contains(t2("e",2)));
        assert(sec.contains(t2("a",2)));

        //old values deleted
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2("d",2)));
        assert(sec.contains(t2("v",2)));
        assert(sec.contains(t2("a",2)));

        //all removed on delete
        m.remove(2);
        assertEquals(5,sec.size());

        //all added on put
        m.put(2,"dva");
        assertEquals(5+3,sec.size());
        assert(sec.contains(t2("d",2)));
        assert(sec.contains(t2("v",2)));
        assert(sec.contains(t2("a",2)));

    }

}
