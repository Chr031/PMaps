package com.pmaps.pmap.pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;

public class PartPair<K, V> implements DataStructure, Pair<K, V> {

	/**
	 * 
	 */
	private final PMap<K, V> pMap;

	/**
	 * The position of this Pair in the file.
	 */
	long pairPointer;

	/**
	 * The length of the serialized data (key + value).<br>
	 * Only valid with read constructor of once write has been called
	 */
	int dataLength;
	byte[] serializedKeyValue;
	long nextPairPointer;
	long partPointer;

	private K key;
	private V value;

	//private Map<Long, Integer> partDataSpaceMap = new HashMap<>();
	private final List<PartPairData> partDataSpaceList;
	private int totalPartSpace;

	public PartPair(PMap<K, V> pMap, long pairPointer, boolean b) {
		this.pMap = pMap;
		this.pairPointer = pairPointer;
		partDataSpaceList = new ArrayList<>();
		totalPartSpace = 0;
	}
	
	
	/**
	 * <p>
	 * <b>Read constructor.</b> <br>
	 * Reads a partitioned pair which header starts at pairPointer.
	 * </p>
	 * See method {@link #write(FileDriver)} before changing anything there.
	 * 
	 * @param pMap
	 * @param pairPointer
	 * @throws IOException
	 */
	public PartPair(PMap<K, V> pMap, long pairPointer) throws IOException {
		this(pMap,pairPointer, true);
		final FileDriver fileDriver = pMap.getPairFileDriver();		
		read(fileDriver);
	}

	/**
	 * <b>Write constructor.</b>
	 * 
	 * @param pairPointer
	 * @param key
	 * @param value
	 * @param pMap
	 *            TODO
	 */
	public PartPair(PMap<K, V> pMap, long pairPointer, K key, V value) {
		this(pMap,pairPointer, true);
		this.key = key;
		this.value = value;
		nextPairPointer = PMap.EOF_POSITION;

	}

	/**
	 * 
	 */
	public void read(FileDriver fileDriver) throws IOException {

		final PartPairHeader header = new PartPairHeader(fileDriver, pairPointer);
		serializedKeyValue = new byte[header.dataLength];

		dataLength = header.dataLength;
		nextPairPointer = header.nextPairPointer;
		partPointer = header.partPointer;

		long nextPartPointer = header.partPointer;
		int dataOffset = 0;
		while (nextPartPointer != PMap.EOF_POSITION) {
			final PartPairData partData = new PartPairData(fileDriver, nextPartPointer);
			System.arraycopy(partData.getData(), 0, serializedKeyValue, dataOffset, partData.getPartLength());
			dataOffset += partData.getPartLength();
			nextPartPointer = partData.getNextPartPointer();
			addPartData(partData.getPartPointer(), partData.getPartLength());
		}

	}

	/**
	 * Write this pair at the position #pairPointer.
	 * 
	 * @param fileDriver
	 * @throws IOException
	 */
	public void write(FileDriver fileDriver) throws IOException {

		// register the missing byte for the data
		if (totalPartSpace < getDataLength()) {
			addPartData(fileDriver.length(), getDataLength() - totalPartSpace);
		}

		byte[] serialized;
		serialized = serializedKeyValue == null ? this.pMap.getSerializer().serialize(key, value) : serializedKeyValue;

		
		
		int offset = 0;
		for (int i=0;i<partDataSpaceList.size();i++) {
			PartPairData part = partDataSpaceList.get(i);
			part.setData(Arrays.copyOfRange(serialized, offset, offset += part.getPartLength()));
			//System.arraycopy(serialized, offset, part.getData(), 0, part.getPartLength());
			//offset += part.getPartLength();
			if (i>0)
				partDataSpaceList.get(i-1).setNextPartPointer(part.getPartPointer());
		}
		
		
		// set the first pointer
		final long partPointer = partDataSpaceList.get(0).getPartPointer();

		final PartPairHeader header = new PartPairHeader(pairPointer, nextPairPointer, dataLength, partPointer);

		// save everything
		for (PartPairData part : partDataSpaceList) {
			part.write(fileDriver);
		}
		header.write(fileDriver);

	}

	/**
	 * Should be called only with write constructor and always before
	 * {@link #write(FileDriver)} method !
	 * 
	 * Returns null if the given space is total accepted and a resulting {@link PartPairData}a if the space is not totally accepted.
	 * In case of a non null return, this means that the {@link PartPair} is completely covered by {@link PartPairData} 
	 * 
	 * @param partDataPointer
	 * @param partDataLenght
	 * @return
	 * @throws IOException
	 */
	protected PartPairData addPartData(long partDataPointer, int partDataLength) throws IOException {

		
		boolean accepted = totalPartSpace <= getDataLength();
		if (accepted) {
			final int localPartDataLength = Math.min(partDataLength, getDataLength() - totalPartSpace);
			
			final int cutOffset = partDataLength - localPartDataLength;
			partDataSpaceList.add(new PartPairData(partDataPointer+cutOffset, localPartDataLength));
			totalPartSpace += localPartDataLength;
			
			if (cutOffset>PartPairData.HEADER_BYTE_SIZE*3) {
				return new PartPairData(partDataPointer, cutOffset-(int)PartPairData.HEADER_BYTE_SIZE);
			} else return null;
		}

		return new PartPairData(partDataPointer, partDataLength);
	}

	public K getKey() throws IOException, ClassNotFoundException {
		if (key == null)
			getObjects();

		return key;
	}

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
		return 4 + dataLength + 8;
	}

	/**
	 * Compute the data length if necessary !!!
	 */
	public int getDataLength() throws IOException {
		if (serializedKeyValue == null) {
			// means pair was created with write constructor
			serializedKeyValue = this.pMap.getSerializer().serialize(key, value);
			dataLength = serializedKeyValue.length;
		}
		return dataLength;
	}

	@Override
	public long getPairPointer() {
		return pairPointer;
	}

	@Override
	public void setPairPointer(long pairPointer) {
		this.pairPointer = pairPointer;

	}

	@Override
	public long getNextPairPointer() {
		return nextPairPointer;
	}

	@Override
	public void setNextPairPointer(long nextPairPointer) {
		this.nextPairPointer = nextPairPointer;

	}

	protected PMap<K,V> getPMap() {
		return pMap;
	}


	public long getPartPointer() {
		return partPointer;
	}
	
	
	public PartPairData getPartPairData(int index) {
		return partDataSpaceList.get(index);
	}
	
	public int getPartPairDataCount() {
		return partDataSpaceList.size();
	}

}
