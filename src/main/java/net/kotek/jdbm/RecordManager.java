package net.kotek.jdbm;

/**
 * @author Jan Kotek
 */
public interface RecordManager {

    <A> long recordPut(A value, Serializer<A> serializer);

    <A> A recordGet(long recid, Serializer<A> serializer);

    <A> void recordUpdate(long recid, A value, Serializer<A> serializer);

    void recordDelete(long recid);

    Long getNamedRecid(String name);

    void setNamedRecid(String name, Long recid);

    void close();

}
