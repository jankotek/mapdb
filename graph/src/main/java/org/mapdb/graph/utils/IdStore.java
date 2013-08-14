package org.mapdb.graph.utils;

import org.mapdb.Atomic;
import org.mapdb.DB;

import java.util.Map;

public class IdStore {

    private final Atomic.Long uidGenerator;
    private final Map<Long, Object> uids;
    private final Map<Object, Long> ids;

    public IdStore(String name, DB db) {
        this.uidGenerator = buildUidGenerator(db, "UID_GENERATOR");
        this.uids = buildUidIndex(db, name + "-UID_INDEX");
        this.ids = buildIdIndex(db, name + "-ID_INDEX");
    }

    private Atomic.Long buildUidGenerator(DB db, String name) {
        if (db.exists(name)) return db.getAtomicLong(name);
        else return db.createAtomicLong(name, 0);
    }

    private Map<Long, Object> buildUidIndex(DB db, String name) {
        return db.createHashMap(name).makeOrGet();
    }

    private Map<Object, Long> buildIdIndex(DB db, String name) {
        return db.createHashMap(name).makeOrGet();
    }

    public Long generateUid(Object id) {
        if (hasId(id)) return getUid(id);
        long uid = uidGenerator.getAndIncrement();
        ids.put(id, uid);
        uids.put(uid, id);
        return uid;
    }

    public boolean hasId(Object id) {
        return ids.containsKey(id);
    }

    public void removeId(Object id) {
        Long uid = ids.remove(id);
        if (uid != null) uids.remove(uid);
    }

    public void removeUid(Long uid) {
        Object id = uids.remove(uid);
        if (id != null) ids.remove(id);
    }

    public Long getUid(Object id) {
        return ids.get(id);
    }

    public Object getId(Long uid) {
        return uids.get(uid);
    }

}
