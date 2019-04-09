package org.mapdb.ec.flat.primitiveEquals

    fun equals(type:String, left:String, right:String ) = when(type) {
        "boolean"-> operatorEquals(left, right)
        "byte" -> operatorEquals(left, right)
        "short" -> operatorEquals(left, right)
        "char" -> operatorEquals(left, right)
        "int" -> operatorEquals(left, right)
        "long" -> operatorEquals(left, right)
        "float" -> floatEquals(left, right)
        "double" -> doubleEquals(left, right)
        else -> throw Error("no matching equals")
    }

    fun operatorEquals(left:String, right:String ) = "$left == $right"
    

    fun doubleEquals(left:String, right:String ) = "Double.compare($left, $right) == 0"

    fun floatEquals(left:String, right:String) = "Float.compare($left, $right>) == 0"

    fun notEquals(type:String, left:String, right:String) = when(type) {
        "boolean" -> operatorNotEquals(left, right)
        "byte" -> operatorNotEquals(left, right)
        "short" -> operatorNotEquals(left, right)
        "char" -> operatorNotEquals(left, right)
        "int" -> operatorNotEquals(left, right)
        "long" -> operatorNotEquals(left, right)
        "float" -> floatNotEquals(left, right)
        "double" -> doubleNotEquals(left, right)
        else -> throw Error("no matching not equals")
    }

    fun operatorNotEquals(left:String, right:String) = "$left != $right"

    fun doubleNotEquals(left:String, right:String) = "Double.compare($left>, $right) != 0"

    fun floatNotEquals(left:String, right:String) = "Float.compare($left, $right) != 0"

    fun lessThan(type:String, left:String, right:String) = when(type) {
        "byte" -> operatorLessThan(left, right)
        "short" -> operatorLessThan(left, right)
        "char" -> operatorLessThan(left, right)
        "int" -> operatorLessThan(left, right)
        "long" -> operatorLessThan(left, right)
        "float" -> floatLessThan(left, right)
        "double" -> doubleLessThan(left, right)
        else -> throw Error("no matching less than")
    }

    fun operatorLessThan(left:String, right:String) = "$left < $right"

    fun doubleLessThan(left:String, right:String) = "Double.compare($left, $right) < 0"
    
    fun floatLessThan(left:String, right:String) = "Float.compare($left, $right) < 0"

    fun greaterThanOrEquals(type:String, left:String, right:String) = when(type) {
        "byte" -> operatorGreaterThanOrEquals(left, right)
        "short" -> operatorGreaterThanOrEquals(left, right)
        "char" -> operatorGreaterThanOrEquals(left, right)
        "int" -> operatorGreaterThanOrEquals(left, right)
        "long" -> operatorGreaterThanOrEquals(left, right)
        "float" -> floatGreaterThanOrEquals(left, right)
        "double" -> doubleGreaterThanOrEquals(left, right)
        else -> throw Error("no matching greater than or equals")
    }

    fun operatorGreaterThanOrEquals(left:String, right:String) = "$left >= $right"


    fun doubleGreaterThanOrEquals(left:String, right:String) = "Double.compare($left, $right) >= 0"

    fun floatGreaterThanOrEquals(left:String, right:String) = "Float.compare($left, $right) >= 0"


    fun lessThanOrEquals(type:String, left:String, right:String) = when(type) {
        "byte" -> operatorLessThanOrEquals(left, right)
        "short" -> operatorLessThanOrEquals(left, right)
        "char" -> operatorLessThanOrEquals(left, right)
        "int" -> operatorLessThanOrEquals(left, right)
        "long" -> operatorLessThanOrEquals(left, right)
        "float" -> floatLessThanOrEqual(left, right)
        "double" -> doubleLessThanOrEquals(left, right)
        else -> throw Error("no matching less than or equals")
    }

    fun operatorLessThanOrEquals(left:String, right:String) = "$left <= $right"

    fun doubleLessThanOrEquals(left:String, right:String) = "Double.compare($left, $right) <= 0"

    fun floatLessThanOrEqual(left:String, right:String)  = "Float.compare($left, $right) <= 0"
