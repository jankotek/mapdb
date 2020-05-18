package org.mapdb;


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



    public static class StoreReentry extends DBException {
        public StoreReentry() {
            super("repeated call to Store method");
        }
    }

    public static class FileLocked extends DBException {
        public FileLocked() {
            super("file locked");
        }
    }

    public static class PreallocRecordAccess extends DBException{
        public PreallocRecordAccess(){
            super("preallocated record accessed");
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

    public static class WrongConfig extends DBException {

        public WrongConfig(String msg) {
            super(msg);
        }
    }

    public static class WrongSerializer extends WrongConfig{
        public WrongSerializer(){
            super("wrong serializer used");
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

    public static class RecordNotPreallocated extends DBException {

        public RecordNotPreallocated() {
            super("Record was not preallocated");
        }
    }
}