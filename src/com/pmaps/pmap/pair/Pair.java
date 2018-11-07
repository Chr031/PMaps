package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.filedriver.FileDriver;

public interface Pair<K, V> {

	/**
	 * Write this pair at the position #pairPointer.
	 * 
	 * @param fileDriver
	 * @throws IOException
	 */
	public abstract void write(FileDriver fileDriver) throws IOException;

	public abstract K getKey() throws IOException, ClassNotFoundException;

	public abstract V getValue() throws IOException, ClassNotFoundException;

	public abstract long getPairPointer();

	public abstract void setPairPointer(long pairPointer);

	public abstract long getNextPairPointer();

	public abstract void setNextPairPointer(long nextPairPointer);

	public abstract int getDataLength() throws IOException;

}