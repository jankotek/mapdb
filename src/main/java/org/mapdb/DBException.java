package org.mapdb;


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

    public static class RecordNotFound extends DBException{
        public RecordNotFound(){
            super("record not found");
        }
    }


    public static class RecidNotFound extends DBException{
        public RecidNotFound(){
            super("recid not found");
        }
    }

    public static class StoreClosed extends DBException{
        public StoreClosed(){
            super("store closed");
        }
    }


    public static class DataCorruption extends DBException{
        public DataCorruption(){
            super("data corruption");
        }

        public DataCorruption(String msg) {
            super(msg);
        }
    }

    public static class SerializationError extends DBException{
        public SerializationError(Exception e){
            super(e);
        }
    }


    public static class PointerChecksumBroken extends DataCorruption{

    }

    public static class TODO extends DBException{

        public TODO() {
            super("not implemented yet");
        }

        public TODO(String msg) {
            super(msg);
        }
    }
}