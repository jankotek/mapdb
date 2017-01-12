package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TT;

import java.io.Serializable;
import java.util.ArrayList;

public class Issue794 {

    public static class DataPoint extends ArrayList<String> implements Serializable
    {
        private static final long serialVersionUID = -3660080037783514247L;
        public DataPoint()
        {
            super(5);
        }
    }

    double limit = TT.testScale()*1e5 + 100;

    @Test
    public void test() {
        DB db = DBMaker.memoryDB().closeOnJvmShutdown().make();
        DB.TreeMapSink<Long, DataPoint> sIds2DataPointSink =
                db.treeMap("ids2DataPoint", Serializer.LONG, Serializer.ELSA).valuesOutsideNodesEnable().createFromSink();
        for (long i=0;i<limit;i++)
        {
            DataPoint point = new DataPoint();
            point.add("aaa");
            point.add("aaa");
            point.add("aaa");
            point.add("aaa");

            //insert 70G DataPoint entities, each filled with 5 Strings
            sIds2DataPointSink.put(i, point);
        }
        //here the error occurs
        sIds2DataPointSink.create();
        db.close();
    }
}
