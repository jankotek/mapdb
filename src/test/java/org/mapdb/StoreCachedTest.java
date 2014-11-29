package org.mapdb;


import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;
import static org.mapdb.StoreDirect.*;

@SuppressWarnings({"rawtypes","unchecked"})
public class StoreCachedTest<E extends StoreCached> extends EngineTest<E>{

    @Override boolean canRollback(){return false;}

    File f = UtilsTest.tempDbFile();


//    static final long FREE_RECID_STACK = StoreDirect.IO_FREE_RECID+32;

    @Override protected E openEngine() {
        return (E) new StoreCached(f.getPath());
    }

}
