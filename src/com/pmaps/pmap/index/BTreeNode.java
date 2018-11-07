package com.pmaps.pmap.index;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Enumeration;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;
import com.pmaps.pmap.pair.Pair;
import com.pmaps.pmap.pair.PairEnumeration;
import com.pmaps.pmap.pair.PlainPair;

/**
 * Node structure of the index.
 * 
 * 
 * TODO {@link BTreeNode} and {@link PlainPair} could implement the same
 * interface with read and write methods.
 * 
 * Node structure : <code>
 * [beforeTreePointer:long | keyHash1:int | nextPair1Pointer:long | centerTreePointer:long | keyHash2:int | nextPair2Pointer:long | afterTreePointer:long]
 * </code>
 * 
 * @author Bleu
 * 
 * @param <Kp>
 * @param <Vp>
 */
public class BTreeNode<K, V> {

	public final static int BYTE_SIZE = 8 + 4 + 8 + 8 + 4 + 8 + 8; // 48

	private final PMap<K, V> pMap;

	final long position;

	long beforeTreePointer;
	int keyHash1;
	long nextPair1Pointer;
	long centerTreePointer;
	int keyHash2;
	long nextPair2Pointer;
	long afterTreePointer;

	/**
	 * <p>
	 * <b>Read constructor.</b>
	 * </p>
	 * 
	 * @param pMap
	 * @param position
	 * @throws IOException
	 */
	public BTreeNode(PMap<K, V> pMap, long position) throws IOException {

		this.pMap = pMap;
		final FileDriver fileDriver = pMap.getIndexFileDriver();
		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(position);

			this.position = position;

			beforeTreePointer = fileDriver.getLong();
			keyHash1 = fileDriver.readInt();
			nextPair1Pointer = fileDriver.getLong();

			centerTreePointer = fileDriver.getLong();

			keyHash2 = fileDriver.readInt();
			nextPair2Pointer = fileDriver.getLong();
			afterTreePointer = fileDriver.getLong();
		}

	}

	/**
	 * *
	 * <p>
	 * <b>Write constructor.</b>
	 * </p>
	 * Call {@link #write(RandomAccessFile)} to write this {@link BTreeNode} to
	 * the raf.
	 * 
	 * @param pMap
	 * @param keyHash
	 * @param position
	 */
	public BTreeNode(PMap<K, V> pMap, int keyHash, long position) {
		this.pMap = pMap;

		this.position = position;

		this.keyHash1 = this.keyHash2 = keyHash;

		beforeTreePointer = nextPair1Pointer = centerTreePointer = nextPair2Pointer = afterTreePointer = PMap.EOF_POSITION;
	}

	public Enumeration<Pair<K, V>> getPairEnumeration(final int keyHash) {
		long nextPairPointer;

		if (keyHash == keyHash1)
			nextPairPointer = nextPair1Pointer;
		else if (keyHash == keyHash2)
			nextPairPointer = nextPair2Pointer;
		else
			nextPairPointer = PMap.EOF_POSITION;

		return new PairEnumeration<K, V>(pMap, nextPairPointer);

	}

	public void write(FileDriver fileDriver) throws IOException {

		synchronized (fileDriver.getAccessLock()) {
			fileDriver.seek(this.position);

			fileDriver.writeLong(beforeTreePointer);
			fileDriver.writeInt(keyHash1);
			fileDriver.writeLong(nextPair1Pointer);
			fileDriver.writeLong(centerTreePointer);
			fileDriver.writeInt(keyHash2);
			fileDriver.writeLong(nextPair2Pointer);
			fileDriver.writeLong(afterTreePointer);
		}

	}

	/**
	 * When registering a new node, this node given in parameter has to be a one
	 * value node and only the keyHash1 is taken into account.
	 * 
	 * @param next
	 */
	public void registerNextIndex(BTreeNode<K, V> next) {

		if (next.keyHash1 < keyHash1)
			beforeTreePointer = next.position;
		else if (next.keyHash1 > keyHash1 && (next.keyHash1 < keyHash2 || keyHash1 == keyHash2))
			centerTreePointer = next.position;
		else if (next.keyHash1 > keyHash2) {
			afterTreePointer = next.position;
		}

	}

	public void setNextPairPointer(int keyHash, long pairPointer) {
		if (keyHash == keyHash1)
			nextPair1Pointer = pairPointer;
		else if (keyHash == keyHash2)
			nextPair2Pointer = pairPointer;
	}

	@Override
	public String toString() {
		return "BTreeNode [position=" + position + ", beforeTreePointer=" + beforeTreePointer + ", keyHash1=" + keyHash1 + ", nextPair1Pointer="
				+ nextPair1Pointer + ", centerTreePointer=" + centerTreePointer + ", keyHash2=" + keyHash2 + ", nextPair2Pointer=" + nextPair2Pointer
				+ ", afterTreePointer=" + afterTreePointer + "]";
	}

}
