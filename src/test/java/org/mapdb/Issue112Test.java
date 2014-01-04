package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Issue112Test {


        @Test(timeout=10000)
        public void testDoubleCommit() throws Exception {
            final DB myTestDataFile = DBMaker.newFileDB(UtilsTest.tempDbFile())
                    .checksumEnable()
                    .make();
            myTestDataFile.commit();
            myTestDataFile.commit();

            long recid = myTestDataFile.engine.put("aa",Serializer.STRING_NOSIZE);
            myTestDataFile.commit();

            assertEquals("aa",myTestDataFile.engine.get(recid, Serializer.STRING_NOSIZE));
        }

    }
