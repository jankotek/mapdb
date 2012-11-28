package org.mapdb;


import org.junit.Ignore;

@Ignore
public class TestUtils {

    public static void perfResult(Class clazz, String key, Object value){
        System.out.println("PERF: "+clazz.getSimpleName()+"-"+key+" = "+value);
    }

}
