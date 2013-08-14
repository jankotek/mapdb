package org.mapdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.NavigableSet;

public class Issue89Test {


        private static final String MY_TEST_DATA_FILE = Utils.tempDbFile().getAbsolutePath();
        private static final String MAP_DB_DATA_FILE_TO_REMOVE = MY_TEST_DATA_FILE + ".0";
        private static final String TEST_TREE_SET = "TestTreeSet";
        private static final String DUMMY_CONTENT = "DummyContent";


        @Before
        public void setUp() throws Exception {
            deleteFile();
        }

        @After
        public void tearDown() throws Exception {
            deleteFile();
        }


        @Test
        public void testAppend() throws Exception {
            appendToDataFile();
            appendToDataFile();
            appendToDataFile();
            appendToDataFile();
        }


        private void appendToDataFile() {
            final DB myTestDataFile = createMapDB(MY_TEST_DATA_FILE);
            addData(myTestDataFile);
            myTestDataFile.close();
        }


        private void addData(DB myTestDataFile) {
            final NavigableSet<Object> testTreeSet = myTestDataFile.getTreeSet(TEST_TREE_SET);
            testTreeSet.add(DUMMY_CONTENT);
            myTestDataFile.commit();

        }


        private DB createMapDB(String fileName) {
            final File file = new File(fileName);
            return createMapDB(file);
        }


        private DB createMapDB(File file) {
            return DBMaker.newAppendFileDB(file)
                    .closeOnJvmShutdown()
                    .cacheDisable()
                    .asyncWriteDisable()
                    .make();
        }


        private void deleteFile() {
            final File file = new File(MAP_DB_DATA_FILE_TO_REMOVE);
            if (file.exists()) {
                file.delete();
            }
        }


    }
