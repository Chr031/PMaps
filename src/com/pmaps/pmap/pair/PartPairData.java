package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;

/**
 * <ol>
 * Is of the form
 * <li>int : part data length</li>
 * <li>long : next part pointer</li>
 * <li>byte[] : byte array of length 'part data length</li>
 * </ol>
 * 
 * @author Bleu
 *
 */
class PartPairData implements DataStructure {
	
	public static final long HEADER_BYTE_SIZE = 12 ; // 4+8;
	
	private long partPointer;
	private int partLength;
	private long nextPartPointer;
	private byte[] data;

	/**
	 * <b> Read constructor<b>
	 * 
	 * @param pairPointer
	 * @throws IOException
	 */
	public PartPairData(FileDriver fileDriver, long partPointer) throws IOException {
		this.partPointer = partPointer;
		this.read(fileDriver);
	}

	/**
	 * <b> Write constructor</b>
	 * 
	 * @param partPointer
	 * @param partLength
	 * @param nextPartPointer
	 */
	public PartPairData(long partPointer, byte[] data, long nextPartPointer) {
		super();
		this.partPointer = partPointer;
		this.partLength = data.length;
		this.data = data;
		this.nextPartPointer = nextPartPointer;
	}
	
	/**
	 * <b> 2nd Write constructor</b>
	 * 
	 * @param partPointer
	 * @param partLength
	 * @param nextPartPointer
	 */
	public PartPairData(long partPointer, int dataLength) {
		super();
		this.partPointer = partPointer;
		this.partLength = dataLength;
		this.data = null;
		this.nextPartPointer = PMap.EOF_POSITION;
	}
	

	@Override
	public void read(FileDriver fileDriver) throws IOException {
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(partPointer);
			partLength = fileDriver.getInt();
			nextPartPointer = fileDriver.getLong();
			data = new byte[partLength];
			fileDriver.get(data);
		}

	}

	@Override
	public void write(FileDriver fileDriver) throws IOException {
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(partPointer);
			fileDriver.setInt(partLength);
			fileDriver.setLong(nextPartPointer);
			fileDriver.set(data);
		}

	}

	long getPartPointer() {
		return partPointer;
	}

	void setPartPointer(long partPointer) {
		this.partPointer = partPointer;
	}

	int getPartLength() {
		return partLength;
	}

	void setPartLength(int partLength) {
		this.partLength = partLength;
	}

	long getNextPartPointer() {
		return nextPartPointer;
	}

	void setNextPartPointer(long nextPartPointer) {
		this.nextPartPointer = nextPartPointer;
	}

	byte[] getData() {
		return data;
	}

	void setData(byte[] data) {
		this.data = data;
		this.partLength = data.length;
	}
}