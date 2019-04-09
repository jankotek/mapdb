package org.mapdb.ec.flat;

public enum Primitive
{
    INT("int"),
    FLOAT("float"),
    DOUBLE("double"),
    LONG("long"),
    SHORT("short"),
    BYTE("byte"),
    CHAR("char"),
    BOOLEAN("boolean");

    public final String type;

    Primitive(String type)
    {
        this.type = type;
    }

    public String getName()
    {
        return this.type.substring(0, 1).toUpperCase() + this.type.substring(1);
    }

    public String getWrapperName()
    {
        if ("int".equals(this.type))
        {
            return "Integer";
        }
        if ("char".equals(this.type))
        {
            return "Character";
        }
        return this.getName();
    }

    public boolean isIntPrimitive()
    {
        return this == INT;
    }

    public boolean isFloatingPoint()
    {
        return this == FLOAT || this == DOUBLE;
    }

    public boolean isBytePrimitive()
    {
        return this == BYTE;
    }

    public String getByteBufferString()
    {
        return this == BYTE?"":getName();
    }

    public boolean isBooleanPrimitive()
    {
        return this == BOOLEAN;
    }

    public boolean isCharPrimitive()
    {
        return this == CHAR;
    }

    public boolean isShortPrimitive()
    {
        return this == SHORT;
    }

    public boolean isFloatPrimitive()
    {
        return this == FLOAT;
    }

    public boolean isDoublePrimitive()
    {
        return this == DOUBLE;
    }

    public boolean isLongPrimitive()
    {
        return this == LONG;
    }

    public boolean hasSpecializedStream()
    {
        return this == INT || this == LONG || this == DOUBLE;
    }

    public boolean isByteOrShortOrCharPrimitive()
    {
        return this == BYTE || this == SHORT || this == CHAR;
    }

    public boolean isFloatOrDoublePrimitive()
    {
        return this == FLOAT || this == DOUBLE;
    }
}
