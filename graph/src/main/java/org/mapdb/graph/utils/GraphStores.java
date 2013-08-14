package org.mapdb.graph.utils;

import org.mapdb.BTreeKeySerializer;
import org.mapdb.BTreeMap;
import org.mapdb.DB;
import org.mapdb.Fun;

import java.util.Map;
import java.util.Set;

public class GraphStores {

    private static final String VERTEX_STORE = "VERTEX_STORE";
    private static final String EDGE_STORE = "EDGE_STORE";
    private static final String IN_RELATION_STORE = "IN_RELATION_STORE";
    private static final String OUT_RELATION_STORE = "OUT_RELATION_STORE";
    private static final String PROPERTY_STORE = "PROPERTY_STORE";
    // Vertex store
    public final IdStore vertexIdStore;
    public final Set<Long> vertexStore;
    // Edge store
    public final IdStore edgeIdStore;
    public final Map<Long, Fun.Tuple3> edgeStore;
    // IN/OUT relation stores
    public final BTreeMap<Fun.Tuple3, Long> inRelationStore;
    public final BTreeMap<Fun.Tuple3, Long> outRelationStore;
    // Property store
    public final BTreeMap<Fun.Tuple2, Object> propertyStore;
    // MapDB instance
    private final DB db;

    public GraphStores(DB db) {
        this.db = db;
        // Init vertex store
        this.vertexIdStore = new IdStore(VERTEX_STORE, db);
        this.vertexStore = buildVertexStore(db);
        // Init edge store
        this.edgeIdStore = new IdStore(EDGE_STORE, db);
        this.edgeStore = buildEdgeStore(db);
        // Init in/out relation store
        this.inRelationStore = buildInRelationStore(db);
        this.outRelationStore = buildOutRelationStore(db);
        // Init property store
        this.propertyStore = buildPropertyStore(db);
    }

    private Set<Long> buildVertexStore(DB db) {
        if (db.exists(VERTEX_STORE)) return db.getHashSet(VERTEX_STORE);
        else return db.createHashSet(VERTEX_STORE, false, null);
    }

    private Map<Long, Fun.Tuple3> buildEdgeStore(DB db) {
        return db.createHashMap(EDGE_STORE).makeOrGet();
    }

    private BTreeMap<Fun.Tuple3, Long> buildInRelationStore(DB db) {
        return db.createTreeMap(IN_RELATION_STORE)
                .keySerializer(BTreeKeySerializer.TUPLE3)
                .makeOrGet();
    }

    private BTreeMap<Fun.Tuple3, Long> buildOutRelationStore(DB db) {
        return db.createTreeMap(OUT_RELATION_STORE)
                .keySerializer(BTreeKeySerializer.TUPLE3)
                .makeOrGet();
    }

    private BTreeMap<Fun.Tuple2, Object> buildPropertyStore(DB db) {
        return db.createTreeMap(PROPERTY_STORE)
                .keySerializer(BTreeKeySerializer.TUPLE2)
                .makeOrGet();
    }

    public void close() {
        db.close();
    }

}
