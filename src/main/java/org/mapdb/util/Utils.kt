package org.mapdb.util

fun dataAssert(bool:Boolean){
    if(!bool)
        throw AssertionError()
}