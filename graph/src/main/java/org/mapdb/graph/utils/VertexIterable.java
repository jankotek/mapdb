package org.mapdb.graph.utils;

import com.tinkerpop.blueprints.Vertex;
import org.mapdb.graph.MapDBGraph;

import java.util.Iterator;

public class VertexIterable implements Iterable<Vertex> {

    private final Iterable<Long> uids;
    private final MapDBGraph graph;
    private final GraphStores stores;

    public VertexIterable(Iterable<Long> uids, MapDBGraph graph, GraphStores stores) {
        this.uids = uids;
        this.graph = graph;
        this.stores = stores;
    }

    @Override
    public Iterator<Vertex> iterator() {
        return new Iterator<Vertex>() {

            final Iterator<Long> iterator = uids.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Vertex next() {
                Long uid = iterator.next();
                Object key = stores.vertexIdStore.getId(uid);
                return graph.getVertex(key);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

}
