package org.mapdb.ec.flat.primitiveHashCode


    fun hashCode(type:String, item:String) = when(type)
    {
        "boolean" -> booleanHashCode(item)
        "byte" -> narrowHashCode(item)
        "short" -> narrowHashCode(item)
        "char" -> narrowHashCode(item)
        "int" -> intHashCode(item)
        "long" -> longHashCode(item)
        "float" -> floatHashCode(item)
        "double" -> doubleHashCode(item)
        else -> throw Error("no matching hash code: "+type)
    }

    fun booleanHashCode(item:String) =
         "($item ? 1231 : 1237)"
    

    fun intHashCode(item:String) =
        "$item"
    

    fun narrowHashCode(item:String) =
        "(int) $item"

    fun longHashCode(item:String) =
        "(int) ($item ^ $item >> 32)"

    fun floatHashCode(item:String) =
        "Float.floatToIntBits($item)"
    

    fun doubleHashCode(item:String) =
        "(int) (Double.doubleToLongBits($item) ^ Double.doubleToLongBits($item) >> 32)"
    

    fun bits(type:String, item:String) = when(type) {
        "byte" -> narrowHashCode(item)
        "short" -> narrowHashCode(item)
        "char" -> narrowHashCode(item)
        "int" -> intHashCode(item)
        "long" -> intHashCode(item)
        "float" -> floatHashCode(item)
        "double" -> doubleBits(item)
        else -> throw Error("no matching bits: " + type)
    }

    fun doubleBits(item:String) =
        "Double.doubleToLongBits($item)"
    

    fun spread(type:String, item:String) = when(type){
    "byte" -> spread8(item)
    "short" -> spread32(item)
    "char" -> spread32(item)
    "int" -> spread32(item)
    "long" -> spread64(item)
    "float" -> spread32(item)
    "double" -> spread64(item)
    else -> throw Error("no matching spread function: "+type)
    }

    fun spread2(type:String, item:String) = when(type) {
        "byte" -> spread28(item)
        "short" -> spread232(item)
        "char" -> spread232(item)
        "int" -> spread232(item)
        "long" -> spread264(item)
        "float" -> spread232(item)
        "double" -> spread264(item)
        else -> throw Error("no matching spread function: " + type)
    }

    fun reverseSpread(type:String, item:String) = when(type ){
        "byte" -> integerReverse(item)
        "short" -> integerReverse(item)
        "char" -> integerReverse(item)
        "int" -> integerReverse(item)
        "long" -> longReverse(item)
        "float" -> integerReverse(item)
        "double" -> longReverse(item)
        else -> throw Error("no matching spread function: "+type)
    }

    fun integerReverse(item:String) =
        "Integer.reverse($item)"

    fun longReverse(item:String) =
        "(int) Long.reverse($item)"
    

    fun spread8(type:String) = """
    int spreadAndMask($type element)
    {
        // No spreading necessary for 8-bit types
        return this.mask(element);
    }
"""

    fun spread28(type:String) = "" //TODO this was empty in template?
    

    fun spread32(type:String) = """
    int spreadAndMask($type element)
    {
        int code = SpreadFunctions.${type}SpreadOne(element);
        return this.mask(code);
    }
    """

    fun spread232(type:String) = """
    int spreadTwoAndMask($type element)
    {
        int code = SpreadFunctions.${type}SpreadTwo(element);
        return this.mask(code);
    }
"""

    fun spread64(type:String) = """
    int spreadAndMask($type element)
    {
        long code = SpreadFunctions.${type}SpreadOne(element);
        return this.mask((int) code);
    }
"""

    fun spread264(type:String) = """
    int spreadTwoAndMask($type element)
    {
        long code = SpreadFunctions.${type}SpreadTwo(element);
        return this.mask((int) code);
    }
"""

    fun probe(type:String, item:String):String = when(type) {
//        "byte" -> "probe8(item)"
//        "short" -> probe3264(item)
//        "char" -> probe3264(item)
//        "int" -> probe3264(item)
//        "float" -> probe3264(item)
//        "long" -> probe3264(item)
//        "double" -> probe3264(item)
        else -> throw Error("no matching probe function: " + type)
    }


