package org.mapdb.issues;

import org.junit.Test;
import org.mapdb.Atomic;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.TT;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import static org.junit.Assert.assertEquals;

public class IssueFromDatumbox {

    public static class SomeOtherClass {

    }

    public static class SomeObject implements Serializable {
        int someValue = 1;
        Class someClass;
    }

    @Test public void main() throws IOException {

        //Pick one of the following lines to get a different error
        String f = TT.tempFile().getPath(); //fails every time - throws java.lang.NullPointerException
        //File f = File.createTempFile("mapdb","db"); //fails every time - throws java.io.EOFException exception
        //String f = "/tmp/constantName"; //fails only in the first execution but NOT in any subsequent execution - throws java.lang.NullPointerException

        SomeObject x = new SomeObject();
        x.someValue = 10;
        x.someClass = SomeOtherClass.class;

        DB db = DBMaker.fileDB(f).make();
        Atomic.Var<Object> atomicVar = db.atomicVar("test").createOrOpen();

        atomicVar.set(x);
        db.close();

        db = DBMaker.fileDB(f).make();

        atomicVar = db.atomicVar("test").createOrOpen();
        x = (SomeObject) atomicVar.get();
        assertEquals(10, x.someValue);
        assertEquals(SomeOtherClass.class, x.someClass);

        db.close();
    }

}
