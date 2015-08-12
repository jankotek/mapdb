package org.mapdb20;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;

/**
 * General exception returned by MapDB if something goes wrong.
 * Subclasses inform about specific failure.
 *
 */
public class DBException extends RuntimeException{


    public DBException(String message) {
        super(message);
    }

    public DBException(String message, Throwable cause) {
        super(message,cause);
    }

    public DBException() {
        super();
    }


    public static class EngineGetVoid extends DBException{
        public EngineGetVoid(){
            super("Recid passed to Engine.get() does not exist. Possible data corruption!");
        }
    }

    public static class EngineCompactUncommited extends DBException{
        public EngineCompactUncommited(){
            super("Engine.compact() called while there are uncommited data. Commit first, than compact!");
        }
    }

    /** @see java.nio.channels.ClosedByInterruptException */
    //TODO this thread was interrupted while doing IO?
    public static class VolumeClosedByInterrupt extends VolumeClosed{
        public VolumeClosedByInterrupt(ClosedByInterruptException cause){
            super("Some thread was interrupted while doing IO, and FileChannel was closed in result.", cause);
        }
    }

    public static class VolumeClosed extends DBException{
        public VolumeClosed(IOException cause){
            this("Volume (file or other device) was already closed.", cause);
        }

        protected VolumeClosed(String msg, IOException cause) {
            super(msg,cause);
        }
    }


    /** Some other process (possibly DB) holds exclusive lock over this file, so it can not be opened */
    public static class FileLocked extends DBException{

        public FileLocked(String message) {
            super(message);
        }

        public FileLocked(String message, Throwable cause) {
            super(message,cause);
        }
    }

    public static class VolumeIOError extends DBException{
        public VolumeIOError(String msg){
            super(msg);
        }

        public VolumeIOError(String msg, Throwable cause){
            super(msg, cause);
        }

        public VolumeIOError(Throwable cause){
            super("IO failed", cause);
        }
    }

    public static class VolumeEOF extends VolumeIOError {
        public VolumeEOF() {
            this("Beyond End Of File accessed");
        }

        public VolumeEOF(String s) {
            super(s);
        }
    }

    public static class OutOfMemory extends VolumeIOError{
        public OutOfMemory(Throwable e){
            super(
                    e.getMessage().equals("Direct buffer memory")?
                            "Out of Direct buffer memory. Increase it with JVM option '-XX:MaxDirectMemorySize=10G'":
                            e.getMessage(),
                    e);
        }

    }

    public static class DataCorruption extends DBException{
        public DataCorruption(String msg){
            super(msg);
        }

        public DataCorruption() {
            super();
        }
    }

    public static class ChecksumBroken extends DataCorruption{
        public ChecksumBroken(){
            super("CRC checksum is broken");
        }
    }

    public static class HeadChecksumBroken extends DataCorruption{
        public HeadChecksumBroken(){
            super("Head checksum broken, perhaps db was not closed correctly?");
        }
    }

    public static class PointerChecksumBroken extends DataCorruption{
        public PointerChecksumBroken(){
            super("Bit parity in file pointer is broken, data possibly corrupted.");
        }
    }

    public static class Interrupted extends DBException {
        public Interrupted(InterruptedException e) {
            super("Thread interrupted",e);
        }
    }

    public static class PumpSourceDuplicate extends DBException {
        public PumpSourceDuplicate(Object key) {
            super("Duplicate found, use .pumpIgnoreDuplicates() to ignore. Duplicate key:"+key);
        }
    }

    public static class PumpSourceNotSorted extends DBException {
        public PumpSourceNotSorted() {
            super("Source iterator not sorted, use .pumpPresort(10000000) to sort keys.");
        }
    }

    public static class WrongConfig extends DBException{
        public WrongConfig(String message) {
            super(message);
        }

        public WrongConfig(String message, Throwable cause) {
            super(message,cause);
        }
    }

    public static class UnknownSerializer extends DBException{
        public UnknownSerializer(String message) {
            super(message);
        }
    }

    public static class FileDeleteFailed extends DBException {
        public FileDeleteFailed(File file) {
            super("Could not delete file: "+file);
        }
    }

    public static class VolumeMaxSizeExceeded extends DBException {
        public VolumeMaxSizeExceeded(long length, long requestedLength) {
            super("Could not expand store. Maximal store size: "+length+", new requested size: "+requestedLength);
        }
    }

    public static class ClassNotFound extends DBException {
        public ClassNotFound(ClassNotFoundException e) {
            super("Class not found! Check classpath or register your class with DBMaker.serializerRegisterClass()",e);
        }
    }
}
