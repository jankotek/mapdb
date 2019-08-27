package org.mapdb.ser;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.mapdb.ser.Serializers.*;
/**
 * Created by jan on 2/28/16.
 */
public final class SerializerUtils {


    private static Map<Class, Serializer> SERIALIZER_FOR_CLASS = new HashMap();

    static {
            SERIALIZER_FOR_CLASS.put(char.class, CHAR);
            SERIALIZER_FOR_CLASS.put(Character.class, CHAR);
            SERIALIZER_FOR_CLASS.put(String.class, STRING);
            SERIALIZER_FOR_CLASS.put(long.class, LONG);
            SERIALIZER_FOR_CLASS.put(Long.class, LONG);
            SERIALIZER_FOR_CLASS.put(int.class, INTEGER);
            SERIALIZER_FOR_CLASS.put(Integer.class, INTEGER);
            SERIALIZER_FOR_CLASS.put(boolean.class, BOOLEAN);
            SERIALIZER_FOR_CLASS.put(Boolean.class, BOOLEAN);
            SERIALIZER_FOR_CLASS.put(byte[].class, BYTE_ARRAY);
            SERIALIZER_FOR_CLASS.put(char[].class, CHAR_ARRAY);
            SERIALIZER_FOR_CLASS.put(int[].class, INT_ARRAY);
            SERIALIZER_FOR_CLASS.put(long[].class, LONG_ARRAY);
            SERIALIZER_FOR_CLASS.put(double[].class, DOUBLE_ARRAY);
            SERIALIZER_FOR_CLASS.put(UUID.class, UUID);
            SERIALIZER_FOR_CLASS.put(byte.class, BYTE);
            SERIALIZER_FOR_CLASS.put(Byte.class, BYTE);
            SERIALIZER_FOR_CLASS.put(float.class, FLOAT);
            SERIALIZER_FOR_CLASS.put(Float.class, FLOAT);
            SERIALIZER_FOR_CLASS.put(double.class, DOUBLE);
            SERIALIZER_FOR_CLASS.put(Double.class, DOUBLE);
            SERIALIZER_FOR_CLASS.put(short.class, SHORT);
            SERIALIZER_FOR_CLASS.put(Short.class, SHORT);
            SERIALIZER_FOR_CLASS.put(short[].class, SHORT_ARRAY);
            SERIALIZER_FOR_CLASS.put(float[].class, FLOAT_ARRAY);
            SERIALIZER_FOR_CLASS.put(BigDecimal.class, BIG_DECIMAL);
            SERIALIZER_FOR_CLASS.put(BigInteger.class, BIG_INTEGER);
            SERIALIZER_FOR_CLASS.put(Class.class, CLASS);
            SERIALIZER_FOR_CLASS.put(Date.class, DATE);

    }


    public static <R> Serializer<R> serializerForClass(Class<R> clazz){
        return SERIALIZER_FOR_CLASS.get(clazz);
    }

    public static int compareInt(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }



}
