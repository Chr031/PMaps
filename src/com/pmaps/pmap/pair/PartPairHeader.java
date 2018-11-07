package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.filedriver.FileDriver;

/**
 * 
 * <ol>
 * Is of the form
 * <li>long : next pair pointer</li>
 * <li>int : total length of the data</li>
 * <li>long : next part pointer</li>
 * 
 * @author Bleu
 *
 */
class PartPairHeader implements DataStructure {

	public static final long BYTE_SIZE = 20; // 8 + 4 + 8

	long headerPointer;
	long nextPairPointer;
	int dataLength;
	long partPointer;

	/**
	 * <b> Read constructor<b>
	 * 
	 * @param pairPointer
	 * @throws IOException
	 */
	public PartPairHeader(FileDriver fileDriver, long headerPointer) throws IOException {
		this.headerPointer = headerPointer;
		this.read(fileDriver);
	}

	/**
	 * <b>Write constructor</b>
	 * 
	 * @param headerPointer
	 * @param nextPairPointer
	 * @param dataLength
	 * @param partPointer
	 */
	public PartPairHeader(long headerPointer, long nextPairPointer, int dataLength, long partPointer) {
		super();
		this.headerPointer = headerPointer;
		this.nextPairPointer = nextPairPointer;
		this.dataLength = dataLength;
		this.partPointer = partPointer;
	}

	@Override
	public void read(FileDriver fileDriver) throws IOException {
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(headerPointer);
			nextPairPointer = fileDriver.getLong();
			dataLength = fileDriver.getInt();
			partPointer = fileDriver.getLong();
		}
	}

	@Override
	public void write(FileDriver fileDriver) throws IOException {
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(headerPointer);

			fileDriver.setLong(nextPairPointer);
			fileDriver.setInt(dataLength);
			fileDriver.setLong(partPointer);
		}
	}

}