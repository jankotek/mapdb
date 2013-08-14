package org.mapdb.graph;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;

import java.io.Serializable;

public class MapDBEdge extends MapDBElement implements Edge, Serializable {

    private final String label;
    private final Vertex inVertex;
    private final Vertex outVertex;

    protected MapDBEdge(Object id, Long uid, Vertex outVertex, Vertex inVertex, String label, MapDBGraph graph) {
        super(id, uid, graph);
        this.label = label;
        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    @Override
    public Vertex getVertex(Direction direction) throws IllegalArgumentException {
        if (direction.equals(Direction.IN)) return this.inVertex;
        else if (direction.equals(Direction.OUT)) return this.outVertex;
        else throw ExceptionFactory.bothIsNotSupported();
    }

    @Override
    public void remove() {
        graph.removeEdge(this);
    }

    @Override
    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
