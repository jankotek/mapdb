package org.mapdb.graph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.MultiIterable;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

public class MapDBVertex extends MapDBElement implements Vertex, Serializable {

    protected MapDBVertex(Object id, Long uid, MapDBGraph graph) {
        super(id, uid, graph);
    }

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        HashSet<String> set = new HashSet<String>(Arrays.asList(labels));
        if (direction.equals(Direction.OUT)) {
            return graph.getOutEdges(uid, set);
        } else if (direction.equals(Direction.IN))
            return graph.getInEdges(uid, set);
        else {
            Iterable<Edge> inEdges = graph.getInEdges(uid, set);
            Iterable<Edge> outEdges = graph.getOutEdges(uid, set);
            return new MultiIterable<Edge>(Arrays.asList(inEdges, outEdges));
        }
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        HashSet<String> set = new HashSet<String>(Arrays.asList(labels));
        if (direction.equals(Direction.OUT)) {
            return graph.getOutVertices(uid, set);
        } else if (direction.equals(Direction.IN))
            return graph.getInVertices(uid, set);
        else {
            Iterable<Vertex> inVertices = graph.getInVertices(uid, set);
            Iterable<Vertex> outVertices = graph.getOutVertices(uid, set);
            return new MultiIterable<Vertex>(Arrays.asList(inVertices, outVertices));
        }
    }

    @Override
    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return graph.addEdge(null, this, inVertex, label);
    }

    @Override
    public void remove() {
        graph.removeVertex(this);
    }

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }

}
