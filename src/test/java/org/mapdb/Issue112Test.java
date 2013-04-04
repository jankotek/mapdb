package org.mapdb;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class Issue112Test {


        @Test
        public void testDoubleCommit() throws Exception {
            final DB myTestDataFile = DBMaker.newFileDB(Utils.tempDbFile())
                    .checksumEnable()
                    .make();
            myTestDataFile.commit();
            myTestDataFile.commit();

            long recid = myTestDataFile.engine.put("aa",Serializer.STRING_SERIALIZER);
            myTestDataFile.commit();

            assertEquals("aa",myTestDataFile.engine.get(recid, Serializer.STRING_SERIALIZER));
        }

    }
