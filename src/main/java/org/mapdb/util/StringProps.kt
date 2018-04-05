package org.mapdb.util

import org.mapdb.DBException

    fun Map<String,String>.getBoolean(key:String):Boolean{
        val value = this[key]
        return when(value){
            "true" -> true
            "false" -> false
            else -> throw DBException.WrongConfig("Not boolean value")
        }
    }

    fun Map<String,String>.getBooleanOrDefault(key:String, default:Boolean):Boolean {
        val value = this[key]
        return when (value) {
            "true" -> true
            "false" -> false
            null -> default
            else -> throw DBException.WrongConfig("Not boolean value")
        }
    }
