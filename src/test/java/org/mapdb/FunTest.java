package org.mapdb;


import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.*;
import static org.mapdb.Fun.Pair;

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

    @Test public void roundUp(){
        assertEquals(0, Fun.roundUp(0,5));
        assertEquals(5, Fun.roundUp(1,5));
        assertEquals(5, Fun.roundUp(2,5));
        assertEquals(5, Fun.roundUp(3,5));
        assertEquals(5, Fun.roundUp(4, 5));
        assertEquals(5, Fun.roundUp(5, 5));
        assertEquals(10, Fun.roundUp(6, 5));
        assertEquals(10, Fun.roundUp(10, 5));
    }

    @Test public void filter(){
        TreeSet<Object[]> set = new TreeSet(Fun.COMPARABLE_ARRAY_COMPARATOR);
        for(int i=0;i<3;i++){
            for(int j=0;j<3;j++){
                set.add(new Object[]{i,j});
            }
        }
        Iterator<Object[]> iter = Fun.filter(set, 2).iterator();

        assertArrayEquals(new Object[]{2, 0}, iter.next());
        assertArrayEquals(new Object[]{2, 1}, iter.next());
        assertArrayEquals(new Object[]{2, 2}, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test public void subfilter_composite_map(){
        Comparator<Object[]> comparator = new Fun.ArrayComparator(
                Fun.COMPARATOR, Fun.COMPARATOR, Fun.COMPARATOR
        );
        TreeSet m = new TreeSet(comparator);

        for(int i=0;i<10;i++){
            for(long j=0;j<10;j++){
                for(long k=0;k<10;k++){
                    m.add(new Object[]{i,j,""+k});
                }
            }
        }
        assertEquals(10*10*10,m.size());

        SortedSet s = m.subSet(
                new Object[]{2,4L},
                new Object[]{2,4L,null}
        );

        assertEquals(10, s.size());
        for(long k=0;k<10;k++){
            assertTrue(m.contains(new Object[]{2,4L,""+k}));
        }
    }
}
