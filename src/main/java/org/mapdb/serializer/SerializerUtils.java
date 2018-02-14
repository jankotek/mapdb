package org.mapdb.serializer;


import java.util.HashMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * Created by jan on 2/28/16.
 */
public final class SerializerUtils {

    private static final Map<Class<?>, Serializer<?>> SERIALIZER_FOR_CLASS = new HashMap<>();
    
    static {       
        put(char.class, Serializers.CHAR);
        put(Character.class, Serializers.CHAR);
        put(String.class, Serializers.STRING);
        put(long.class, Serializers.LONG);
        put(Long.class, Serializers.LONG);
        put(int.class, Serializers.INTEGER);
        put(Integer.class, Serializers.INTEGER);
        put(boolean.class, Serializers.BOOLEAN);
        put(Boolean.class, Serializers.BOOLEAN);
        put(byte[].class, Serializers.BYTE_ARRAY);
        put(char[].class, Serializers.CHAR_ARRAY);
        put(int[].class, Serializers.INT_ARRAY);
        put(long[].class, Serializers.LONG_ARRAY);
        put(double[].class, Serializers.DOUBLE_ARRAY);
        put(UUID.class, Serializers.UUID);
        put(byte.class, Serializers.BYTE);
        put(Byte.class, Serializers.BYTE);
        put(float.class, Serializers.FLOAT);
        put(Float.class, Serializers.FLOAT);
        put(double.class, Serializers.DOUBLE);
        put(Double.class, Serializers.DOUBLE);
        put(short.class, Serializers.SHORT);
        put(Short.class, Serializers.SHORT);
        put(short[].class, Serializers.SHORT_ARRAY);
        put(float[].class, Serializers.FLOAT_ARRAY);
        put(BigDecimal.class, Serializers.BIG_DECIMAL);
        put(BigInteger.class, Serializers.BIG_INTEGER);
        put(Class.class, Serializers.CLASS);
        put(Date.class, Serializers.DATE);
        put(java.sql.Date.class, Serializers.SQL_DATE);
        put(java.sql.Time.class, Serializers.SQL_TIME);
        put(java.sql.Timestamp.class, Serializers.SQL_TIMESTAMP);
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
