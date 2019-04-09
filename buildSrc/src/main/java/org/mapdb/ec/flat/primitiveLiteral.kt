package org.mapdb.ec.flat.primitiveLiteral


    fun literal(type:String, item:String) = when(type){
        "byte" -> byteLiteral(item)
        "short" -> shortLiteral(item)
        "char" -> charLiteral(item)
        "int" -> intLiteral(item)
        "long" -> longLiteral(item)
        "float" -> floatLiteral(item)
        "double" -> doubleLiteral(item)
        else -> throw Error("no matching literal "+type)
    }

    fun intLiteral(item:String) = "${item}"


    fun shortLiteral(item:String) = "(short) ${item}"


    fun byteLiteral(item:String) = "(byte) ${item}"


    fun charLiteral(item:String) = "(char) ${item}"


    fun longLiteral(item:String) = "${item}L"


    fun floatLiteral(item:String) = "${item}.0f"


    fun doubleLiteral(item:String) = "${item}.0"


    fun decimalLiteral(type:String, item:String) = when(type) {
        "float" -> floatDecimalLiteral(item)
        "double" -> doubleDecimalLiteral(item)
        else -> throw Error("type: "+type)
    }

    fun floatDecimalLiteral(item:String) = "${item}f"


    fun doubleDecimalLiteral(item:String) = "${item}"


    fun wideLiteral(type:String, item:String) = when(type){
        "byte" -> longLiteral(item)
        "short" -> longLiteral(item)
        "char" -> longLiteral(item)
        "int" -> longLiteral(item)
        "long" -> longLiteral(item)
        "float" -> doubleLiteral(item)
        "double" -> doubleLiteral(item)
        else -> throw Error("no matching wide literal: "+type)
    }

    fun toStringLiteral(type:String, item:String) = when(type) {
        "byte" -> intLiteral(item)
        "short" -> intLiteral(item)
        "char" -> charStringLiteral(item)
        "int" -> intLiteral(item)
        "long" -> intLiteral(item)
        "float" -> doubleLiteral(item)
        "double" -> doubleLiteral(item)
        else -> throw Error("no matching toString literal: " + type)
    }

    //FIXME wrong str?
    fun charStringLiteral(item:String) = "\\u<item; format=\"%04x\">"


    fun zero(type:String) = when(type){
        "boolean" -> "false"
        "byte" -> "(byte) 0"
        "short" -> "(short) 0"
        "char" -> "'0'" //FIXME zero char
        "int" -> "0"
        "long" -> "0L"
        "float" -> "0.0f"
        "double" -> "0.0"
        else -> throw Error("no matching zero: "+type)
    }

    fun wideZero(type:String) = when(type){
        "byte" -> "0L"
        "short" -> "0L"
        "char" ->"0L"
        "int" -> "0L"
        "long" -> "0L"
        "float" -> "0.0"
        "double" -> "0.0"
        else -> throw Error("no matching wide zero: "+type)
    }

    fun delta(type:String) = when(type){
        "byte" -> ""
        "short" -> ""
        "char" -> ""
        "int" -> ""
        "long" -> ""
        "float" -> "0.0f"
        "double" -> "0.0"
        else -> throw Error("no matching delta "+type)
    }

    fun wideDelta(type:String) = when(type){
        "boolean" -> ""
        "byte" -> ""
        "short" -> ""
        "char" -> ""
        "int" -> ""
        "long" -> ""
        "float" ->  "0.0"
        "double" -> "0.0"
        else -> throw Error("no matching wide delta: "+type)
    }

    fun wideType(type:String) = when(type) {
        "byte" -> "long"
        "short" -> "long"
        "char" -> "long"
        "int" -> "long"
        "long" -> "long"
        "float" -> "double"
        "double" -> "double"
        "boolean" -> "boolean"
        else -> throw Error("no matching wide type: " + type)
    }

    fun wideName(type:String) = when(type){
        "byte" -> "Long"
        "short" -> "Long"
        "char" -> "Long"
        "int" -> "Long"
        "long" -> "Long"
        "float" -> "Double"
        "double" -> "Double"
        "boolean" -> "Boolean"
        else -> throw Error("no matching wide type: "+type)
    }

    fun bitsType(type:String) = when(type){
        "float" -> "int"
        "double" -> "long"
        else -> throw Error("no matching type: "+type)
    }

    fun castWideType(type:String) = when(type) {
        "byte" -> "(long) "
        "short" -> "(long) "
        "char" -> "(long) "
        "int" -> "(long) "
        "long" -> ""
        "float" -> "(double) "
        "double" -> ""
        "boolean" -> ""
        else -> throw Error("no matching wide type: " + type)
    }

    fun castDouble(type:String) = when(type) {
        "byte" -> "(double) "
        "short" -> "(double) "
        "char" -> "(double) "
        "int" -> "(double) "
        "long" -> "(double) "
        "float" -> "(double) "
        "double" -> ""
        else -> throw Error("no matching double cast: " + type)
    }

    fun castSum(type:String) = when(type) {
        "byte" -> "(double) "
        "short" -> "(double) "
        "char" -> "(double) "
        "int" -> "(double) "
        "long" -> "(double) "
        "float" -> ""
        "double" -> ""
        else -> throw Error("no matching sum cast: " + type)
    }

    fun castIntToNarrowTypeWithParens(type:String, item:String) = when(type) {
        "byte" -> byteCastWithParens(item)
        "short" -> shortCastWithParens(item)
        "char" -> charCastWithParens(item)
        "int" -> noCast(item)
        "long" -> noCast(item)
        "float" -> noCast(item)
        "double" -> noCast(item)
        else -> throw Error("no matching narrow cast: " + type)
    }

    fun castIntToNarrowType(type:String, item:String) = when(type){
        "byte" -> byteCast(item)
        "short" -> shortCast(item)
        "char" -> charCast(item)
        "int" -> noCast(item)
        "long" -> noCast(item)
        "float" -> noCast(item)
        "double" -> noCast(item)
        else -> throw Error("no type: " + type)
    }

    fun castLongToNarrowType(type:String, item:String) = when(type) {
        "byte" -> byteCast(item)
        "short" -> shortCast(item)
        "char" -> charCast(item)
        "int" -> intCast(item)
        "long" -> noCast(item)
        "float" -> noCast(item)
        "double" -> noCast(item)
        else -> throw Error("no type: " + type)
    }

    fun castLongToNarrowTypeWithParens(type:String, item:String) = when(type){
        "byte" -> byteCastWithParens(item)
        "short" -> shortCastWithParens(item)
        "char" -> charCastWithParens(item)
        "int" -> intCastWithParens(item)
        "long" -> noCast(item)
        "float" -> noCast(item)
        "double" -> noCast(item)
        else -> throw Error("no type: " + type)
    }

    fun noCast(item:String) = "${item}"


    fun castRealTypeToInt(type:String, item:String) = when(type) {
        "byte" -> noCast(item)
        "short" -> noCast(item)
        "char" -> noCast(item)
        "int" -> noCast(item)
        "long" -> noCast(item)
        "float" -> intCast(item)
        "double" -> intCast(item)
        else -> throw Error("no matching real cast: " + type)
    }

    fun intCast(item:String) = "(int) ${item}"


    fun shortCast(item:String) = "(short) ${item}"


    fun byteCast(item:String) = "(byte) ${item}"


    fun charCast(item:String) = "(char) ${item}"


    fun longCast(item:String) = "(long) ${item}"


    fun floatCast(item:String) = "(float) ${item}"


    fun doubleCast(item:String) = "(double) ${item}"


    fun intCastWithParens(item:String) = "(int) (${item})"


    fun shortCastWithParens(item:String) = "(short) (${item})"


    fun byteCastWithParens(item:String) = "(byte) (${item})"


    fun charCastWithParens(item:String) = "(char) (${item})"


    fun longCastWithParens(item:String) = "(long) (${item})"


    fun floatCastWithParens(item:String) = "(float) (${item})"


    fun doubleCastWithParens(item:String) = "(double) (${item})"


    fun castFromInt(type:String, item:String) = when(type) {
        "byte" -> byteCast(item)
        "short" -> shortCast(item)
        "char" -> charCast(item)
        "int" -> noCast(item)
        "long" -> longCast(item)
        "float" -> floatCast(item)
        "double" -> doubleCast(item)
        else -> throw Error("no matching int cast: " + type)
    }

    fun castFromIntWithParens(type:String, item:String) = when(type) {
        "byte" -> byteCastWithParens(item)
        "short" -> shortCastWithParens(item)
        "char" -> charCastWithParens(item)
        "int" -> noCast(item)
        "long" -> longCastWithParens(item)
        "float" -> floatCastWithParens(item)
        "double" -> doubleCastWithParens(item)
        else -> throw Error("no matching int cast: " + type)
    }

    fun castExactly(type:String, item:String, skip:Boolean) = if(!skip)"(cast.(${type}))(${item})" else item


    fun cast(type:String, item:String) = when(type) {
        "byte" -> byteCast(item)
        "short" -> shortCast(item)
        "char" -> charCast(item)
        "int" -> intCast(item)
        "long" -> longCast(item)
        "float" -> floatCast(item)
        "double" -> doubleCast(item)
        else -> throw Error("no matching cast: " + type)
    }


    //TODO ????
    fun article(type:String) = when(type){
        "int" -> "an"
        else -> throw Error("a")
    }

    fun keySize(type:String) = when(type){
        "byte" -> "1"
        "short" -> "2"
        "char" -> "2"
        "int" -> "4"
        "long" -> "8"
        "float" -> "4"
        "double" -> "8"
        else -> throw Error("no matching key type: "+type)
    }
