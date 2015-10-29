package org.mapdb;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;

import static org.junit.Assert.*;

/**
 * Tests that given mapdb flavour has all settings applied.
 * This test can be removed if you rename mapdb maven artifact
 *
 */
public class MavenFlavourTest {

    @Test
    public void test_flavour() throws IOException, IllegalAccessException {
        RandomAccessFile f = new RandomAccessFile("pom.xml", "r");
        byte[] b = new byte[(int) f.length()];
        f.read(b);


        String mavenContent = new String(b);
        String flavour = mavenContent.split("<[//]*artifactId>")[1];

        System.out.println("Maven flavour: " + flavour);

        if ("mapdb".equals(flavour)) {
            //no checks here
        } else if ("mapdb-renamed".equals(flavour)) {
            assertFalse(this.getClass().toString().contains(".mapdb."));
            assertFalse(new File("src/main/java/org/mapdb").exists());
            assertFalse(new File("src/test/java/org/mapdb").exists());
        } else if ("mapdb-nounsafe".equals(flavour)) {
            try {
                Class.forName("org.mapdb.UnsafeStuff");
                fail();
            } catch (ClassNotFoundException e) {
                //expected
            }
        } else if ("mapdb-noassert".equals(flavour)) {
            assertFalse(CC.ASSERT);
            assertFalse(CC.PARANOID);
        } else if ("mapdb-debug".equals(flavour)) {
            assertTrue(CC.ASSERT);
            assertTrue(CC.PARANOID);
            //all logging options should be on
            for (Field field : CC.class.getDeclaredFields()) {
                if (field.getName().startsWith("LOG_")) {
                    assertEquals(field.getName(), true, field.get(null));
                }
            }
        } else {
            fail("Unknown maven flavour: " + flavour);
        }

    }
}
