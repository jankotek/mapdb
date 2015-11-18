package org.mapdb.issues;

import org.junit.Assert;
import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.util.Iterator;
import java.util.Set;

public class Issue132Test {


    static void expectCount(Set<?> set, int count) {
        Assert.assertEquals(count, count(set.iterator()));
    }

    static int count(final Iterator<?> iterator) {
        int counter = 0;
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
        }
        return counter;
    }

    @Test(timeout=50000)
    public  void test_full() {
        long id= 0;
        for(int count = 0; count < TT.scale()*50; count++) {


            DB db = DBMaker.memoryDB()
                    .checksumEnable().make();



            Set<Long> set = db.hashSet("test");
            db.commit();

            for (int i = 0; i < count; i++) {
                set.add(id++);
                db.commit();
            }
            expectCount(set, count);

            for (int i = 0; i < count; i++) {
                set.add(id++);
                db.rollback();
            }
            expectCount(set, count);

            for (int i = 0; i < count; i++) {
                set.add(id++);
            }
            expectCount(set, count * 2);
            db.commit();
            expectCount(set, count * 2);

            db.close();

        }
    }

    @Test(timeout=10000)
    public  void test_isolate() {
        long id= 0;
        int count = 18;


        DB db = DBMaker.memoryDB()
                .checksumEnable().make();


        Set<Long> set = db.hashSet("test");
        db.commit();

        for (int i = 0; i < count; i++) {
            set.add(id++);
        }
        db.commit();
        expectCount(set, count);

        for (int i = 0; i < count; i++) {
            set.add(id++);
        }
        db.rollback();
        expectCount(set, count);

        for (int i = 0; i < count; i++) {
            set.add(id++);
        }
        expectCount(set, count * 2);
        db.commit();
        expectCount(set, count * 2);

        db.close();

    }


}
