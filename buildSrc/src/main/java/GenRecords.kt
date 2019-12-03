object GenRecords {

    fun makeRecordMakers():String{
        data class R(val type:String, val initVal:String, val ser:String){

            private val gen = if(type!="Var") "" else "<E>"

            val isVar = type=="Var"
            val isNum = type=="Int" || type=="Long"

            val nullConv = if(isVar) "" else "!!"

            val extends = if(isNum)":Number()" else ""

            fun makerName() = type+"RecordMaker"+gen
            fun recName() = type+"Record"+gen
            fun valType() = if(!isVar) type else "E?"


            fun constParams() = if(!isVar) "" else ", private val ser:Serializer<E>"
            fun newParams() = if(!isVar) "" else ", ser:Serializer<E>"
            fun newParams2() = if(!isVar) "" else ", ser"
            fun newGen() = if(!isVar) "" else "<E>"

            fun privSer() = if(isVar) "" else "private val ser="+ser
        }

        val types = listOf(
                R("String", "\"\"", "Serializers.STRING"),
                R("Long", "0L", "Serializers.LONG"),
                R("Int", "0", "Serializers.INTEGER"),
                R("Boolean","false", "Serializers.BOOLEAN"),
                R("Var", "null","")
        )

        val mkClasses = types.map { t->
            """
                    class ${t.makerName()}(private val db:DB, private val name:String ${t.constParams()}){
                    
                        private var initVal:${t.valType()} = ${t.initVal}
    
                        fun init(initialValue:${t.valType()}):${t.makerName()}{
                            initVal = initialValue
                            return this
                        }
    
                        fun make():${t.recName()} {
                            val recid = db.store.put(initVal, ${if(t.isVar) "ser" else t.ser})
                            return ${t.recName()}(db.store, recid ${t.newParams2()}) 
                        }
                        
                    }
                    
                    class ${t.recName()}(private val store:MutableStore, private val recid:Long ${t.constParams()})${t.extends}{

                        ${t.privSer()}
                        
                        ${if(t.isNum)"""
                            

                            fun addAndGet(i:${t.type}):${t.valType()} = 
                                store.updateAndGet(recid, ser, {v:${t.valType()}?-> v!!+i})!!

                            fun getAndAdd(i:${t.type}):${t.valType()} = 
                                store.getAndUpdateAtomic(recid, ser, {v:${t.valType()}?-> v!!+i})!!

                            fun getAndDecrement():${t.valType()} = getAndAdd(-1)

                            fun getAndIncrement():${t.valType()} = getAndAdd(+1)

                            fun decrementAndGet():${t.valType()} = addAndGet(-1)

                            fun incrementAndGet():${t.valType()} = addAndGet(+1)

                            override fun toDouble() = get().toDouble()
                            override fun toFloat() = get().toFloat()
                            override fun toLong() = get().toLong()
                            override fun toInt() = get().toInt()
                            override fun toChar() = get().toChar()
                            override fun toShort() = get().toShort()
                            override fun toByte() = get().toByte()
                            
                        """ else ""}
                        
                        fun get():${t.valType()} = store.get(recid, ser)${t.nullConv}

                        fun set(value:${t.valType()}) = store.update(recid, ser, value)

                        fun getAndSet(value:${t.valType()}):${t.valType()} = store.getAndUpdate(recid, ser, value)${t.nullConv}

                        fun compareAndSet(expectedValue:${t.valType()}, newValue:${t.valType()}):Boolean = 
                            store.compareAndUpdate(recid, ser, expectedValue, newValue)

                        override fun toString():String = get().toString()
                    
                    }
                    
                """.trimIndent()
        }.joinToString("")
        val mkFuns = types.map{t->
            """
                @JvmStatic
                     fun ${t.newGen()} new${t.type}(db:DB,name:String ${t.newParams()}):${t.makerName()} {
                        return ${t.makerName()}(db, name ${t.newParams2()})
                     }
                """.trimIndent()
        }.joinToString("")


        return """
                package org.mapdb.record
                
                import org.mapdb.db.DB
                import org.mapdb.store.MutableStore
                import org.mapdb.ser.Serializer
                import org.mapdb.ser.Serializers
                
                class Records{
                    companion object{
                        $mkFuns
                    }
                }
                
                $mkClasses
                
            """.trimIndent()
    }

}