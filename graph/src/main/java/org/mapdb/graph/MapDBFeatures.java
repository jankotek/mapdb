package org.mapdb.graph;

import com.tinkerpop.blueprints.Features;

public class MapDBFeatures extends Features {

    public MapDBFeatures() {
        // General
        supportsDuplicateEdges = false;
        supportsSelfLoops = false;
        ignoresSuppliedIds = false;
        isPersistent = true;
        isWrapper = false;

        // Property
        supportsVertexProperties = true;
        supportsEdgeProperties = true;
        supportsStringProperty = true;
        supportsBooleanProperty = true;
        supportsDoubleProperty = true;
        supportsFloatProperty = true;
        supportsIntegerProperty = true;
        supportsLongProperty = true;
        supportsPrimitiveArrayProperty = true;
        supportsUniformListProperty = false;
        supportsMixedListProperty = false;
        supportsMapProperty = false;
        supportsSerializableObjectProperty = true;

        // Elements
        supportsVertexIteration = false;
        supportsEdgeIteration = false;
        supportsEdgeRetrieval = true;

        // Index
        supportsIndices = false;
        supportsKeyIndices = false;
        supportsVertexKeyIndex = false;
        supportsEdgeKeyIndex = false;
        supportsVertexIndex = false;
        supportsEdgeIndex = false;

        // Transaction and concurrency
        supportsTransactions = false;
        supportsThreadedTransactions = false;
    }

}
