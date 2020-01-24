package org.mapdb;

import sun.reflect.annotation.ExceptionProxy;

import java.nio.file.Path;

public class DBException extends RuntimeException {

    public DBException(Exception cause){
        super(cause);
    }

    public DBException(String msg){
        super(msg);
    }

    public DBException(String msg, Exception cause){
        super(msg,cause);
    }

    class RecordNotFound extends DBException{
        public RecordNotFound(){
            super("record not found");
        }
    }


    class RecidNotFound extends DBException{
        public RecidNotFound(){
            super("recid not found");
        }
    }

    class StoreClosed extends DBException{
        public StoreClosed(){
            super("store closed");
        }
    }


    class DataCorruption extends DBException{
        public DataCorruption(){
            super("data corruption");
        }
    }

    class SerializerError extends DBException{
        public SerializerError(Exception e){
            super(e);
        }
    }


}