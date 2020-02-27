import org.gradle.internal.impldep.org.apache.commons.io.FileUtils
import java.io.File

object GenRecords {

    fun makeRecordMakers(dir: File){
        data class R(val type:String, val initVal:String, val ser:String){

            val gen = if(type!="Var") "" else "<E>"

            val isVar = type=="Var"
            val isNum = type=="Int" || type=="Long"

            val extends = if(isNum)" extends Number" else ""

            fun recName() = type+"Record"
            fun valType() =
                    if(isNum || type=="Boolean") type.toLowerCase()
                    else if(!isVar) type
                    else "E"


            fun constParams() = if(!isVar) "" else "private final Serializer<E> ser;"
            fun newParams() = if(!isVar) "" else ", Serializer<E> ser";
            fun newParams2() = if(!isVar) "" else ", ser"
            fun constBody() = if(!isVar) "" else "this.ser=ser;"
            fun newGen() = if(!isVar) "" else "<E>"



        }

        val types = listOf(
                R("String", "\"\"", "Serializers.STRING"),
                R("Long", "0L", "Serializers.LONG"),
                R("Int", "0", "Serializers.INTEGER"),
                R("Boolean","false", "Serializers.BOOLEAN"),
                R("Var", "null","ser")
        )

        for(t in types){
            val cont = """
                                package org.mapdb.record;
                                
                                import org.mapdb.db.DB;
                                import org.mapdb.store.Store;
                                import org.mapdb.ser.Serializer;
                                import org.mapdb.ser.Serializers;

                    public class ${t.recName()}${t.gen} ${t.extends}{

                    public static class  Maker${t.gen}{
                    
                       private final DB db;
                       private final String name;
                       
                       private ${t.valType()} initVal = ${t.initVal};
                    
                        ${t.constParams()}
                        public Maker(DB db, String name  ${t.newParams()}){
                            this.db = db;
                            this.name = name;
                            ${t.constBody()}
                        }
                    

                        public Maker init(${t.valType()} initialValue){
                            initVal = initialValue;
                            return this;
                        }
    
                        public ${t.recName()}  make(){
                            Store store = db.getStore();
                            long recid = store.put(initVal, ${if(t.isVar) "ser" else t.ser});
                            return new ${t.recName()}(store, recid ${t.newParams2()}); 
                        }
                        
                    }
                    


                        private final Store store;
                        private final long recid;
                        ${t.constParams()}

                        public ${t.recName()}(Store store, long recid ${t.newParams()}){
                            this.store = store;
                            this.recid = recid;
                            ${t.constBody()}
                        }

                        ${if(t.isNum)"""
                            public ${t.valType()} addAndGet(${t.valType()} i){ 
                                return store.updateAndGet(recid, ${t.ser}, (v)-> v+i);
                            }

                            public ${t.valType()} getAndAdd(${t.valType()} i){ 
                                return store.getAndUpdateAtomic(recid, ${t.ser}, (v)-> v+i);
                            }

                            public ${t.valType()}  getAndDecrement(){return getAndAdd(-1);}

                            public ${t.valType()}  getAndIncrement(){return getAndAdd(+1);}

                            public ${t.valType()}  decrementAndGet(){return addAndGet(-1);}

                            public ${t.valType()}  incrementAndGet(){return addAndGet(+1);}

                            @Override public double doubleValue(){ return (double) get();}
                            @Override public float floatValue(){ return (float) get();}
                            @Override public long longValue(){ return (long) get();}
                            @Override public int intValue(){ return (int) get();}
//                            @Override public char charValue(){ return (char) get();}
                            @Override public short shortValue(){ return (short) get();}
                            @Override public byte byteValue(){ return (byte) get();}
                            
                        """ else ""}
                        
                        //TODO hash code
                        
                        public ${t.valType()} get(){
                          return store.get(recid, ${t.ser});
                        }

                        public void set(${t.valType()} value){
                            store.update(recid, ${t.ser}, value);
                        } 

                        public ${t.valType()} getAndSet(${t.valType()} value){
                            return store.getAndUpdate(recid, ${t.ser}, value);
                        }

                        public boolean compareAndSet(${t.valType()} expectedValue, ${t.valType()} newValue){ 
                            return store.compareAndUpdate(recid, ${t.ser}, expectedValue, newValue);
                        }
                        
                        @Override 
                        public String toString(){
                            return ""+get();
                        }
                    
                    }

                """.trimIndent()
            FileUtils.write(File(dir, t.type+"Record.java"), cont);
        }





    }

}