package com.pmaps.pmap.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Manage the serialization of the objects. 
 * Direct serializer for Object that extends the {@link Serializable} interface
 * 
 * @author bleu
 * 
 */
public class JavaSerializer implements ISerializer {

	public byte[] serialize(Object... serials) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(baos);
		try {
			for (Object serial : serials) {
				oos.writeObject(serial);
			}
		} finally {
			oos.close();
		}
		return baos.toByteArray();
	}

	public Object[] unserialize(int numberOfObjects, byte[] b) throws IOException, ClassNotFoundException {
		Serializable[] serials = new Serializable[numberOfObjects];
		ByteArrayInputStream bais = new ByteArrayInputStream(b);
		ObjectInputStream ois = new ObjectInputStream(bais);
		try {
			for (int i = 0; i < serials.length; i++) {
				serials[i] = (Serializable) ois.readObject();
			}
		} finally {
			ois.close();
			bais.close();
		}
		return serials;

	}

}