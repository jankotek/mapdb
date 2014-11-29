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
public class StoreCachedTest<E extends StoreCached> extends StoreDirectTest<E>{

    @Override boolean canRollback(){return false;}

    File f = UtilsTest.tempDbFile();


    @Override protected E openEngine() {
        return (E) new StoreCached(f.getPath());
    }

}
