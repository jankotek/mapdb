package org.mapdb.graph;

import com.tinkerpop.blueprints.*;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.commons.configuration.Configuration;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.Fun;
import org.mapdb.graph.utils.EdgeIterable;
import org.mapdb.graph.utils.GraphStores;
import org.mapdb.graph.utils.VertexIterable;

import java.io.File;
import java.util.*;

import static org.mapdb.Fun.Tuple2;
import static org.mapdb.Fun.Tuple3;

public class MapDBGraph implements Graph {

    private static final Features FEATURES = new MapDBFeatures();
    private final GraphStores stores;

    public MapDBGraph(Configuration configuration) {
        // Retrieve properties
        String file = configuration.getString("blueprints.mapdb.file", "./graph.db");
        boolean isDirectMemory = configuration.getBoolean("blueprints.mapdb.isDirectMemory", false);
        boolean isCompressionEnabled = configuration.getBoolean("blueprints.mapdb.isCompressionEnabled", true);
        boolean isReadOnly = configuration.getBoolean("blueprints.mapdb.isReadOnly", false);
        // Build DB
        DBMaker dbMaker = isDirectMemory ? DBMaker.newDirectMemoryDB() : DBMaker.newFileDB(new File(file));
        dbMaker = isCompressionEnabled ? dbMaker.compressionEnable() : dbMaker;
        dbMaker = isReadOnly ? dbMaker.readOnly() : dbMaker;
        DB db = dbMaker.closeOnJvmShutdown().make();
        // Create GraphStore
        this.stores = new GraphStores(db);
    }

    public static Graph open(Configuration configuration) {
        return new MapDBGraph(configuration);
    }

    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    @Override
    public Vertex addVertex(Object id) {
        if (id == null) id = UUID.randomUUID().toString();
        if (stores.vertexIdStore.hasId(id)) throw ExceptionFactory.vertexWithIdAlreadyExists(id);
        Long uid = stores.vertexIdStore.generateUid(id); // Generate UID
        stores.vertexStore.add(uid);
        return new MapDBVertex(id, uid, this);
    }

    @Override
    public Vertex getVertex(Object id) {
        if (id == null) throw ExceptionFactory.vertexIdCanNotBeNull();
        if (stores.vertexIdStore.hasId(id)) {
            Long uid = stores.vertexIdStore.getUid(id);
            return new MapDBVertex(id, uid, this);
        } else return null;
    }

    @Override
    public void removeVertex(Vertex vertex) {
        Long uid = stores.vertexIdStore.getUid(vertex.getId());
        // Remove Vertex
        stores.vertexStore.remove(uid);
        // Remove related edges (relations)
        Tuple3<Long, Long, String> outRelationFrom = Fun.t3(uid, null, null);
        Tuple3<Long, Long, String> outRelationTo = Fun.t3(uid + 1, null, null);
        // Remove edges (and related out and in relations)
        Map<Tuple3, Long> outMap = stores.outRelationStore.subMap(outRelationFrom, true, outRelationTo, false);
        for (Long edgeUid : outMap.values()) removeEdge(edgeUid);
        // Remove properties
        Tuple2<Long, Object> propertyFrom = Fun.t2(uid, null);
        Tuple2<Long, Object> propertyTo = Fun.t2(uid + 1, null);
        stores.propertyStore.subMap(propertyFrom, true, propertyTo, false).clear();
        // Remove vertex UID
        stores.vertexIdStore.removeUid(uid);
    }

    @Override
    public Iterable<Vertex> getVertices() {
        Iterable<Long> uids = stores.vertexStore;
        return new VertexIterable(uids, this, stores);
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        // Needs a properties reverse index
        throw new UnsupportedOperationException();
    }

    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) {
        if (id == null) id = UUID.randomUUID().toString();
        if (label == null) throw ExceptionFactory.edgeLabelCanNotBeNull();
        if (stores.edgeIdStore.hasId(id)) throw ExceptionFactory.edgeWithIdAlreadyExist(id);
        Long uid = stores.edgeIdStore.generateUid(id); // Generate UID
        Long outVertexUid = stores.vertexIdStore.getUid(outVertex.getId());
        Long inVertexUid = stores.vertexIdStore.getUid(inVertex.getId());
        stores.edgeStore.put(uid, Fun.t3(outVertexUid, inVertexUid, label));
        stores.outRelationStore.put(Fun.t3(outVertexUid, inVertexUid, label), uid);
        stores.inRelationStore.put(Fun.t3(inVertexUid, outVertexUid, label), uid);
        return new MapDBEdge(id, uid, outVertex, inVertex, label, this);
    }

    @Override
    public Edge getEdge(Object id) {
        if (id == null) throw ExceptionFactory.edgeIdCanNotBeNull();
        if (stores.edgeIdStore.hasId(id)) {
            Long uid = stores.edgeIdStore.getUid(id);
            Tuple3<Long, Long, String> vertices = stores.edgeStore.get(uid);
            Long outVertexUid = vertices.a;
            Long inVertexUid = vertices.b;
            Object outVertexId = stores.vertexIdStore.getId(outVertexUid);
            Object inVertexId = stores.vertexIdStore.getId(inVertexUid);
            Vertex outVertex = new MapDBVertex(outVertexId, outVertexUid, this);
            Vertex inVertex = new MapDBVertex(inVertexId, inVertexUid, this);
            String label = vertices.c;
            return new MapDBEdge(id, uid, outVertex, inVertex, label, this);
        } else {
            return null;
        }
    }

    @Override
    public void removeEdge(Edge edge) {
        Long uid = stores.edgeIdStore.getUid(edge.getId());
        removeEdge(uid);
    }

    private void removeEdge(Long uid) {
        // Remove edge
        Tuple3<Long, Long, String> t = stores.edgeStore.remove(uid);
        // Remove in and out relations
        stores.outRelationStore.remove(Fun.t3(t.a, t.b, t.c));
        stores.inRelationStore.remove(Fun.t3(t.b, t.a, t.c));
        // Remove properties
        Tuple2<Long, Object> edgeFrom = Fun.t2(uid, null);
        Tuple2<Long, Object> edgeTo = Fun.t2(uid + 1, null);
        stores.propertyStore.subMap(edgeFrom, true, edgeTo, false).clear();
        // Remove edge UID
        stores.edgeIdStore.removeUid(uid);
    }

    @Override
    public Iterable<Edge> getEdges() {
        Iterable<Long> uids = stores.edgeStore.keySet();
        return new EdgeIterable(uids, this, stores);
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        // Needs a properties reverse index
        throw new UnsupportedOperationException();
    }

    @Override
    public GraphQuery query() {
        return new DefaultGraphQuery(this);
    }

    @Override
    public void shutdown() {
        stores.close();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + stores.vertexStore.size() + " edges:" + stores.edgeStore.size());
    }

    /*
     * Iterators
     */

    Iterable<Vertex> getOutVertices(Long uid, Set<String> labels) {
        Tuple3<Long, Long, String> relationFrom = Fun.t3(uid, null, null);
        Tuple3<Long, Long, String> relationTo = Fun.t3(uid + 1, null, null);
        Map<Tuple3, Long> relations = stores.outRelationStore.subMap(relationFrom, true, relationTo, false);
        List<Vertex> vertices = new ArrayList<Vertex>();
        for (Map.Entry<Tuple3, Long> relation : relations.entrySet()) {
            if (!labels.isEmpty() && !labels.contains(relation.getKey().c)) continue;
            Long vertexUid = (Long) relation.getKey().b;
            Object vertexId = stores.vertexIdStore.getId(vertexUid);
            vertices.add(new MapDBVertex(vertexId, vertexUid, this));
        }
        return vertices;
    }

    Iterable<Vertex> getInVertices(Long uid, Set<String> labels) {
        Tuple3<Long, Long, String> relationFrom = Fun.t3(uid, null, null);
        Tuple3<Long, Long, String> relationTo = Fun.t3(uid + 1, null, null);
        Map<Tuple3, Long> relations = stores.inRelationStore.subMap(relationFrom, true, relationTo, false);
        List<Vertex> vertices = new ArrayList<Vertex>();
        for (Map.Entry<Tuple3, Long> relation : relations.entrySet()) {
            if (!labels.isEmpty() && !labels.contains(relation.getKey().c)) continue;
            Long vertexUid = (Long) relation.getKey().b;
            Object vertexId = stores.vertexIdStore.getId(vertexUid);
            vertices.add(new MapDBVertex(vertexId, vertexUid, this));
        }
        return vertices;
    }

    Iterable<Edge> getOutEdges(Long uid, HashSet<String> labels) {
        Tuple3<Long, Long, String> relationFrom = Fun.t3(uid, null, null);
        Tuple3<Long, Long, String> relationTo = Fun.t3(uid + 1, null, null);
        Map<Tuple3, Long> relations = stores.outRelationStore.subMap(relationFrom, true, relationTo, false);
        List<Edge> edges = new ArrayList<Edge>(relations.size());
        for (Map.Entry<Tuple3, Long> relation : relations.entrySet()) {
            if (!labels.isEmpty() && !labels.contains(relation.getKey().c)) continue;
            Long edgeUid = relation.getValue();
            Object edgeId = stores.edgeIdStore.getId(edgeUid);
            Long outVertexUid = uid;
            Long inVertexUid = (Long) relation.getKey().b;
            Object outVertexId = stores.vertexIdStore.getId(outVertexUid);
            Object inVertexId = stores.vertexIdStore.getId(inVertexUid);
            Vertex outVertex = new MapDBVertex(outVertexId, outVertexUid, this);
            Vertex inVertex = new MapDBVertex(inVertexId, inVertexUid, this);
            String label = (String) relation.getKey().c;
            edges.add(new MapDBEdge(edgeId, edgeUid, outVertex, inVertex, label, this));
        }
        return edges;
    }

    Iterable<Edge> getInEdges(Long uid, HashSet<String> labels) {
        Tuple3<Long, Long, String> relationFrom = Fun.t3(uid, null, null);
        Tuple3<Long, Long, String> relationTo = Fun.t3(uid + 1, null, null);
        Map<Tuple3, Long> relations = stores.inRelationStore.subMap(relationFrom, true, relationTo, false);
        List<Edge> edges = new ArrayList<Edge>(relations.size());
        for (Map.Entry<Tuple3, Long> relation : relations.entrySet()) {
            if (!labels.isEmpty() && !labels.contains(relation.getKey().c)) continue;
            Long edgeUid = relation.getValue();
            Object edgeId = stores.edgeIdStore.getId(edgeUid);
            Long outVertexUid = (Long) relation.getKey().b;
            Long inVertexUid = uid;
            Object outVertexId = stores.vertexIdStore.getId(outVertexUid);
            Object inVertexId = stores.vertexIdStore.getId(uid);
            Vertex outVertex = new MapDBVertex(outVertexId, outVertexUid, this);
            Vertex inVertex = new MapDBVertex(inVertexId, inVertexUid, this);
            String label = (String) relation.getKey().c;
            edges.add(new MapDBEdge(edgeId, edgeUid, outVertex, inVertex, label, this));
        }
        return edges;
    }

    /*
     * Properties
     */

    Object setProperty(Long uid, String key, Object value) {
        if (value instanceof Collection) throw new IllegalArgumentException("Collection values are not supported");
        if (value instanceof Map) throw new IllegalArgumentException("Map values are not supported");
        return stores.propertyStore.put(Fun.t2(uid, key), value);
    }

    Object getProperty(Long uid, String key) {
        return stores.propertyStore.get(Fun.t2(uid, key));
    }

    <V> Map<String, V> getProperties(Long uid) {
        Tuple2<Long, Object> fromKey = Fun.t2(uid, null);
        Tuple2<Long, Object> toKey = Fun.t2(uid + 1, null);
        Map<Tuple2, Object> subMap = stores.propertyStore.subMap(fromKey, true, toKey, false);
        Map<String, V> properties = new HashMap<String, V>();
        for (Map.Entry<Tuple2, Object> entry : subMap.entrySet()) {
            properties.put((String) entry.getKey().b, (V) entry.getValue());
        }
        return properties;
    }

    Object removeProperty(Long uid, String key) {
        return stores.propertyStore.remove(Fun.t2(uid, key));
    }

}
