package org.mapdb.issues;


import org.junit.Test;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Serializer;
import org.mapdb.TT;

import static org.junit.Assert.assertEquals;

public class Issue112Test {


        @Test(timeout=10000)
        public void testDoubleCommit() throws Exception {
            final DB myTestDataFile = DBMaker.fileDB(TT.tempDbFile())
                    .checksumEnable()
                    .make();
            myTestDataFile.commit();
            myTestDataFile.commit();

            long recid = myTestDataFile.getEngine().put("aa", Serializer.STRING_NOSIZE);
            myTestDataFile.commit();

            assertEquals("aa",myTestDataFile.getEngine().get(recid, Serializer.STRING_NOSIZE));
        }

    }
