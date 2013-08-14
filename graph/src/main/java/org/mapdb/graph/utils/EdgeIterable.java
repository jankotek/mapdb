package org.mapdb.graph.utils;

import com.tinkerpop.blueprints.Edge;
import org.mapdb.graph.MapDBGraph;

import java.util.Iterator;

public class EdgeIterable implements Iterable<Edge> {

    private final Iterable<Long> uids;
    private final MapDBGraph graph;
    private final GraphStores stores;

    public EdgeIterable(Iterable<Long> uids, MapDBGraph graph, GraphStores stores) {
        this.uids = uids;
        this.graph = graph;
        this.stores = stores;
    }

    @Override
    public Iterator<Edge> iterator() {
        return new Iterator<Edge>() {

            final Iterator<Long> iterator = uids.iterator();

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Edge next() {
                Long uid = iterator.next();
                Object key = stores.edgeIdStore.getId(uid);
                return graph.getEdge(key);
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

        };
    }

}
