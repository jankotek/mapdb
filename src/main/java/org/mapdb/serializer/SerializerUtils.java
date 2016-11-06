package org.mapdb.serializer;


import java.util.HashMap;

import org.mapdb.Serializer;
import static org.mapdb.Serializer.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by jan on 2/28/16.
 */
public final class SerializerUtils {

    private static final Map<Class<?>, Serializer<?>> SERIALIZER_FOR_CLASS = new HashMap<>();
    
    static {       
        put(char.class, CHAR);
        put(Character.class, CHAR);
        put(String.class, STRING);
        put(long.class, LONG);
        put(Long.class, LONG);
        put(int.class, INTEGER);
        put(Integer.class, INTEGER);
        put(boolean.class, BOOLEAN);
        put(Boolean.class, BOOLEAN);
        put(byte[].class, BYTE_ARRAY);
        put(char[].class, CHAR_ARRAY);
        put(int[].class, INT_ARRAY);
        put(long[].class, LONG_ARRAY);
        put(double[].class, DOUBLE_ARRAY);
        put(UUID.class, UUID);
        put(byte.class, BYTE);
        put(Byte.class, BYTE);
        put(float.class, FLOAT);
        put(Float.class, FLOAT);
        put(double.class, DOUBLE);
        put(Double.class, DOUBLE);
        put(short.class, SHORT);
        put(Short.class, SHORT);
        put(short[].class, SHORT_ARRAY);
        put(float[].class, FLOAT_ARRAY);
        put(BigDecimal.class, BIG_DECIMAL);
        put(BigInteger.class, BIG_INTEGER);
        put(Class.class, CLASS);
        put(Date.class, DATE);
        put(java.sql.Date.class, SQL_DATE);
        put(java.sql.Time.class, SQL_TIME);
        put(java.sql.Timestamp.class, SQL_TIMESTAMP);
    }
    
    // Make sure we are type safe!
    private static <T> void put(Class<? super T> clazz, Serializer<T> serializer) {
        SERIALIZER_FOR_CLASS.put(clazz, serializer);
    } 
    

    public static <R> Serializer<R> serializerForClass(Class<R> clazz) {
        @SuppressWarnings("unchecked")
        final Serializer<R> result = (Serializer<R>) SERIALIZER_FOR_CLASS.get(clazz);
        return result;
    }

    public static int compareInt(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    private SerializerUtils() {
        throw new UnsupportedOperationException("Instance of utility class not allowed");
    }

}
