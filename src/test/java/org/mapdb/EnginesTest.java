package org.mapdb;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
/**
 * Tests contract of various implementations of Engine interface
 */
@RunWith(Parameterized.class)
public class EnginesTest{

    protected final Engine e;

    public EnginesTest(Engine e) {
        this.e = e;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {DBMaker.newMemoryDB().makeEngine()},
                {DBMaker.newMemoryDB().journalDisable().makeEngine()},
                {DBMaker.newAppendFileDB(Utils.tempDbFile()).makeEngine()},
        });
    }

    @Test public void dummy(){}


}
