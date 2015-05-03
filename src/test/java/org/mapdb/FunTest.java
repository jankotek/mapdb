package org.mapdb;


import java.util.Comparator;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.mapdb.Fun.*;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class FunTest {

    public int compare(int[] o1, int[] o2) {
        for(int i = 0;i<o1.length;i++){
            if(o1[i]<o2[i]) return -1;
            if(o1[i]>o2[i]) return 1;
        }
        return 0;
    }


    final Object[] vals = new Object[]{ "A", "B", "C",};

    @Test public void t2_equals(){
        assertEquals(new Pair("A","B"), new Pair("A","B"));
        assertEquals(new Pair("A",null), new Pair("A",null));
        assertFalse(new Pair("A","B").equals(new Pair("A","C")));
    }

    @Test public void t2_compare(){

        for(int a=0;a<vals.length;a++){
            for(int b=0;b<vals.length;b++){
                for(int c=0;c<vals.length;c++){
                    for(int d=0;d<vals.length;d++){
                        Pair t1 = new Pair(vals[a], vals[b]);
                        Pair t2 = new Pair(vals[c], vals[d]);

                        int i = t1.compareTo(t2);

                        //System.out.println(t1 + " - "+t2 + " - "+i);

                        if(a==b && c==d && a==c)
                            assertEquals(0, i);

                        if(a<c || (a==c && b<d))
                            assertTrue(i<0);

                        if(a>c || (a==c && b>d))
                            assertTrue(i>0);

                    }
                }
            }
        }
    }




    @Test public void byte_array_comparator(){
        byte[] b1 = new byte[]{1,1};
        byte[] b1_ = new byte[]{1,1};
        byte[] b2 = new byte[]{1,2};
        byte[] blong = new byte[]{1,2,3};
        assertEquals(-1, Fun.BYTE_ARRAY_COMPARATOR.compare(b1,b2));
        assertEquals(-1, Fun.BYTE_ARRAY_COMPARATOR.compare(b2,blong));
        assertEquals(1, Fun.BYTE_ARRAY_COMPARATOR.compare(b2,b1));
        assertEquals(0, Fun.BYTE_ARRAY_COMPARATOR.compare(b1,b1));
        assertEquals(0, Fun.BYTE_ARRAY_COMPARATOR.compare(b1, b1_));
    }
    
    @Test
    public void getComparator(){
    	Comparator<String> stringComparator = Fun.comparator();
    	String a = "A";
    	String a1 = "A";
    	String b= "B";
    	
    	assertEquals(0, stringComparator.compare(a, a1));
    	assertEquals(-1, stringComparator.compare(a, b));
    	assertEquals(1, stringComparator.compare(b, a));
    }
    
    @Test
    public void getReveresedComparator(){
    	Comparator<String> stringComparator = Fun.reverseComparator();
    	String a = "A";
    	String a1 = "A";
    	String b= "B";
    	
    	assertEquals(0, stringComparator.compare(a, a1));
    	assertEquals(1, stringComparator.compare(a, b));
    	assertEquals(-1, stringComparator.compare(b, a));
    }

}
