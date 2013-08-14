package org.mapdb;


import org.junit.Test;

import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

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


    final Object[] vals = new Object[]{null, "A", "B", "C",  HI};

    @Test public void t2_equals(){
        assertEquals(new Tuple2("A","B"), new Tuple2("A","B"));
        assertEquals(new Tuple2("A",null), new Tuple2("A",null));
        assertEquals(new Tuple2("A",HI), new Tuple2("A",HI));
        assertEquals(new Tuple2(null,HI), new Tuple2(null,HI));

        assertFalse(new Tuple2("A",HI).equals(new Tuple2("A", null)));
        assertFalse(new Tuple2("A","B").equals(new Tuple2("A","C")));
    }

    @Test public void t2_compare(){

        for(int a=0;a<vals.length;a++){
            for(int b=0;b<vals.length;b++){
                for(int c=0;c<vals.length;c++){
                    for(int d=0;d<vals.length;d++){
                        Tuple2 t1 = new Tuple2(vals[a], vals[b]);
                        Tuple2 t2 = new Tuple2(vals[c], vals[d]);

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

    @Test public void t3_compare(){

        for(int a1=0;a1<vals.length;a1++){
            for(int a2=0;a2<vals.length;a2++){
            for(int a3=0;a3<vals.length;a3++){
                for(int b1=0;b1<vals.length;b1++){
                    for(int b2=0;b2<vals.length;b2++){
                    for(int b3=0;b3<vals.length;b3++){

                        Tuple3 a = new Tuple3(vals[a1], vals[a2], vals[a3]);
                        Tuple3 b = new Tuple3(vals[b1], vals[b2], vals[b3]);

                        int i = a.compareTo(b);
                        int i0 = -b.compareTo(a);

                        //System.out.println(a + " - "+ b + " - "+i);

                        int i2 = compare(new int[]{a1,a2,a3}, new int[]{b1,b2,b3});
                        assertEquals(Math.signum(i), Math.signum(i2),1e-10);
                        assertEquals(Math.signum(i0), Math.signum(i2),1e-10);

                    }
                }
            }
        }
        }
        }
    }


    @Test public void t4_compare(){


        for(int a1=0;a1<vals.length;a1++){
            for(int a2=0;a2<vals.length;a2++){
                for(int a3=0;a3<vals.length;a3++){
                    for(int a4=0;a4<vals.length;a4++){
                    for(int b1=0;b1<vals.length;b1++){
                        for(int b2=0;b2<vals.length;b2++){
                            for(int b3=0;b3<vals.length;b3++){
                                for(int b4=0;b4<vals.length;b4++){


                                Tuple4 a = new Tuple4(vals[a1], vals[a2], vals[a3], vals[a4]);
                                Tuple4 b = new Tuple4(vals[b1], vals[b2], vals[b3], vals[b4]);

                                int i = a.compareTo(b);
                                int i0 = -b.compareTo(a);

                                //System.out.println(a + " - "+ b + " - "+i);

                                int i2 = compare(new int[]{a1,a2,a3,a4}, new int[]{b1,b2,b3,b4});
                                assertEquals(Math.signum(i), Math.signum(i2),1e-10);
                                assertEquals(Math.signum(i0), Math.signum(i2),1e-10);

                            }
                        }
                    }
                }
            }
            }}
        }
    }


    @Test public void lo_hi(){
        assertTrue(t2("A",null).compareTo(t2("A",HI))<0);
        assertTrue(t3("A", null, null).compareTo(t3("A", HI, null))<0);
        assertTrue(t4("A", null, null, null).compareTo(t4("A", HI, null, null))<0);
    }

    @Test public void testSubMap(){
        int nums[] = {1,2,3,4,5};
        ConcurrentNavigableMap m = new ConcurrentSkipListMap();
        for(int a:nums) for(int b:nums){
            m.put(t2(a,b),"");
        }

        assertEquals(5, m.subMap(t2(3, null), t2(3,HI)).size());
        assertEquals(3, m.subMap(t2(3, 3), t2(3,HI)).size());

        assertEquals(10, m.headMap(t2(3, null)).size());
        assertEquals(15, m.tailMap(t2(3, null)).size());

        assertEquals(10, m.headMap(t2(2, HI)).size());
        assertEquals(15, m.tailMap(t2(2, HI)).size());

    }

}
