package com.pmaps.pmap.pair;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map.Entry;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;

/**
 * This class is the equivalent of the {@link Entry} class. It represents a
 * tuple of key and value.
 * 
 * @author Bleu
 * 
 * @param <Kp>
 * @param <Vp>
 */
public class PlainPair<K, V> implements Pair<K, V> {

	/**
	 * 
	 */
	private final PMap<K, V> pMap;

	/**
	 * The position of this Pair in the file.
	 */
	private long pairPointer;

	/**
	 * The length of the serialized data (key + value).<br>
	 * Only valid with read constructor of once write has been called
	 */
	private int dataLenght;
	private byte[] serializedKeyValue;
	private long nextPairPointer;

	private K key;
	private V value;

	/**
	 * <p>
	 * <b>Read constructor.</b> <br>
	 * Reads the pair present at the position {@link #pairPointer}. Note that it
	 * reads the key/value bytes for a later de-serialization.
	 * </p>
	 * See method {@link #write(RandomAccessFile)} before changing anything
	 * there.
	 * 
	 * @param pMap
	 * @param pairPointer
	 * @throws IOException
	 */
	protected PlainPair(PMap<K, V> pMap, long pairPointer) throws IOException {
		this.pMap = pMap;
		this.pairPointer = pairPointer;
		final FileDriver fileDriver = pMap.getPairFileDriver();
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(pairPointer);
			dataLenght = fileDriver.readInt();
			serializedKeyValue = new byte[dataLenght];
			fileDriver.read(serializedKeyValue);
			setNextPairPointer(fileDriver.getLong());
		}
	}

	/**
	 * <b>Write constructor.</b>
	 * 
	 * @param pMap
	 * @param pairPointer
	 * @param key
	 * @param value
	 */
	protected PlainPair(PMap<K, V> pMap, long pairPointer, K key, V value) {
		this.pMap = pMap;
		this.pairPointer = pairPointer;
		this.key = key;
		this.value = value;
		setNextPairPointer(PMap.EOF_POSITION);

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.xneb.utils.pmaps.pmap.pair.IPair#write(com.xneb.utils.pmaps.pmap.
	 * filedriver.FileDriver)
	 */
	@Override
	public void write(FileDriver fileDriver) throws IOException {
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(pairPointer);
			byte[] serialized;
			serialized = serializedKeyValue == null ? this.pMap.getSerializer().serialize(key, value) : serializedKeyValue;
			dataLenght = serialized.length;
			fileDriver.writeInt(serialized.length);
			fileDriver.write(serialized);
			fileDriver.writeLong(nextPairPointer);
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#getKey()
	 */
	@Override
	public K getKey() throws IOException, ClassNotFoundException {
		if (key == null)
			getObjects();

		return key;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#getValue()
	 */
	@Override
	public V getValue() throws IOException, ClassNotFoundException {
		if (value == null)
			getObjects();

		return value;
	}

	@SuppressWarnings("unchecked")
	private void getObjects() throws IOException, ClassNotFoundException {
		Object[] unserialized = this.pMap.getSerializer().unserialize(2, serializedKeyValue);
		key = (K) unserialized[0];
		value = (V) unserialized[1];
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#getPairPointer()
	 */
	@Override
	public long getPairPointer() {
		return pairPointer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#setPairPointer(long)
	 */
	@Override
	public void setPairPointer(long pairPointer) {
		this.pairPointer = pairPointer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#getNextPairPointer()
	 */
	@Override
	public long getNextPairPointer() {
		return nextPairPointer;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#setNextPairPointer(long)
	 */
	@Override
	public void setNextPairPointer(long nextPairPointer) {
		this.nextPairPointer = nextPairPointer;
	}

	/**
	 * Method is only working with a read constructor or once write has been
	 * called on this instance.
	 * 
	 * @return
	 */
	protected long getByteSize() {
		// first 4 bytes are for the length of serialized keys and values
		// next bytes are for the data serialized
		// next 8 bytes are for the next pair pointer (long)
		return 4 + dataLenght + 8;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.xneb.utils.pmaps.pmap.pair.IPair#getDataLength()
	 */
	@Override
	public int getDataLength() throws IOException {
		if (serializedKeyValue == null) {
			// means pair was created with write constructor
			serializedKeyValue = this.pMap.getSerializer().serialize(key, value);
			dataLenght = serializedKeyValue.length;
		}
		return dataLenght;
	}

}