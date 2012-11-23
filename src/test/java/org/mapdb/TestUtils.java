package org.mapdb;



public class TestUtils {

    public static void perfResult(Class clazz, String key, Object value){
        System.out.println("PERF: "+clazz.getSimpleName()+"-"+key+" = "+value);
    }

}
