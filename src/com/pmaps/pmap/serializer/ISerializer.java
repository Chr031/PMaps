package com.pmaps.pmap.serializer;

import java.io.IOException;

public interface ISerializer {
	
	byte[] serialize(Object... serials) throws IOException;

	Object[] unserialize(int numberOfObjects, byte[] b) throws IOException, ClassNotFoundException;
}