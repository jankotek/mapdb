package org.mapdb;

/**
 * General exception returned by MapDB if something goes wrong.
 * Check {@link org.mapdb.DBException.Code error code} for more details.
 *
 */
public class DBException extends RuntimeException{


    public static enum Code{

        ENGINE_GET_VOID("Recid passed to Engine.get() does not exist. Possible data corruption!"),

        ENGINE_COMPACT_UNCOMMITED("Engine.compact() called while uncommited data exist. Commit first, than compact!");

        private final String message;

        Code(String message) {
            this.message = message;
        }

        public String getMessage(){
            return message;
        }


        @Override
        public String toString() {
            return super.toString()+" - "+message;
        }
    }


    protected final Code code;

    public DBException(Code code) {
        super(code.toString());
        this.code = code;
    }

    public DBException(Code code, Exception cause) {
        super(code.toString(),cause);
        this.code = code;
    }


    /**
     * @return error code associated with this exception
     */
    public Code getCode(){
        return code;
    }

}
