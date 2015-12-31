package org.mapdb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class TestSerializer extends Serializer {

	@Override
	public void serialize(DataOutput out, Object value) throws IOException {

	}

	@Override
	public Object deserialize(DataInput in, int available) throws IOException {
		return null;
	}
}
