package org.mapdb.ec.flat.primitiveCompare



    fun compare(type:String, left:String, right:String, wrapperName:String) = when(type) {
        "boolean" -> booleanCompare(left, right, wrapperName)
        "byte" -> narrowCompare(left, right, wrapperName)
        "short" -> narrowCompare(left, right, wrapperName)
        "char" -> narrowCompare(left, right, wrapperName)
        "int" -> wideCompare(left, right, wrapperName)
        "long" -> wideCompare(left, right, wrapperName)
        "float" -> wrapperCompare(left, right, wrapperName)
        "double" -> wrapperCompare(left, right, wrapperName)
        else -> throw Error("no matching Compare " + type)
    }

    fun booleanCompare(left:String , right:String , wrapperName:String ) = 
        "$left == $right ? 0 : <left> ? 1 : -1"
    
    fun narrowCompare(left:String , right:String , wrapperName:String ) =
        "$left - $right"


    fun wideCompare(left:String , right:String , wrapperName:String ) =
        "$left < $right ? -1 : $left > $right ? 1 : 0"

    fun wrapperCompare(left:String , right:String , wrapperName:String ) =
        "$wrapperName>.compare($left, $right)"

