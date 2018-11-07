package com.pmaps.pmap.filedriver;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.index.BTreeNode;

public class RandomAccessCachedFileDriver implements FileDriver {

	private final static long POSITION_FILE_LENGTH = PMap.POSITION_FREE_LONG_VALUE;
	private final static long POSITION_INDEX_START = PMap.POSITION_START_DATA;
	
	/**
	 * makes byte buffers of 1Mb roughly
	 */
	private final static int NUMBER_OF_INDEX_NODE_PER_CACHEDBYTEBUFFER = 21800;
	private final static int INDEX_NODE_MAP_UNIT_SIZE = NUMBER_OF_INDEX_NODE_PER_CACHEDBYTEBUFFER * BTreeNode.BYTE_SIZE;

	private class ByteBufferOffset {
		private final ByteBuffer byteBuffer;
		private final long offset;
		public ByteBufferOffset(ByteBuffer byteBuffer, long offset) {
			super();
			this.byteBuffer = byteBuffer;
			this.offset = offset;
		}
		public long arrayOffset() {return offset;}
		
	}

	private final RandomAccessFile raf;
	private final Object accessLock;
	private final FileChannel fileChannel;
	// private final ByteBuffer cachedByteBuffer;
	private final List<ByteBufferOffset> cachedByteBufferList;

	private ByteBufferOffset currentByteBufferOffset;

	public RandomAccessCachedFileDriver(File file) throws IOException {
		if (!file.exists()) {

		}
		raf = new RandomAccessFile(file, "rw");
		accessLock = new Object();
		fileChannel = raf.getChannel();
		cachedByteBufferList = new ArrayList<>();
		final MappedByteBuffer headerByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0, POSITION_INDEX_START);
		final ByteBufferOffset headerBBO = new ByteBufferOffset(headerByteBuffer, 0);
		cachedByteBufferList.add(headerBBO);
		// cachedByteBuffer = fileChannel.map(MapMode.READ_WRITE, 0,
		// POSITION_INDEX_START);
		currentByteBufferOffset = headerBBO;

	}
	
	private ByteBufferOffset getCachedByteBuffer(long position) throws IOException {
		// get the index of the cached byte buffer store in the
		// cachedByteBufferList
		int byteBufferListIndex = 0;
		
		if (position >= POSITION_INDEX_START) {
			byteBufferListIndex = (int) ((position - POSITION_INDEX_START) / INDEX_NODE_MAP_UNIT_SIZE) + 1;
			// offset = (byteBufferListIndex -1) * INDEX_NODE_MAP_UNIT_SIZE +
			// POSITION_INDEX_START;
			if (byteBufferListIndex >= cachedByteBufferList.size()) {
				// creates new buffers
				for (int i = cachedByteBufferList.size(); i <= byteBufferListIndex; i++) {

					long bufferPosition = (i - 1) * INDEX_NODE_MAP_UNIT_SIZE + POSITION_INDEX_START;
					final MappedByteBuffer map = fileChannel.map(MapMode.READ_WRITE, bufferPosition, INDEX_NODE_MAP_UNIT_SIZE);
					cachedByteBufferList.add(new ByteBufferOffset(map,bufferPosition) );
				}
			}

		}

		return cachedByteBufferList.get(byteBufferListIndex);
	}

	private ByteBufferOffset getCurrentByteBuffer() {
		return currentByteBufferOffset;
	}

	@Override
	public Object getAccessLock() {
		return accessLock;
	}

	@Override
	public long length() throws IOException {
		return getLong(POSITION_FILE_LENGTH);
	}

	@Override
	public void setLength(long newLength) throws IOException {
		setLong(POSITION_FILE_LENGTH, newLength);

	}

	@Override
	public void seek(long position) throws IOException {
		currentByteBufferOffset = getCachedByteBuffer(position);
		currentByteBufferOffset.byteBuffer.position((int)(position - currentByteBufferOffset.arrayOffset()));	

	}

	@Override
	public int getInt() throws IOException {
		return getCurrentByteBuffer().byteBuffer.getInt();
	}

	@Override
	public int readInt() throws IOException {
		return getCurrentByteBuffer().byteBuffer.getInt();
	}

	@Override
	public void setInt(int i) throws IOException {
		getCurrentByteBuffer().byteBuffer.putInt(i);

	}

	@Override
	public void writeInt(int i) throws IOException {
		getCurrentByteBuffer().byteBuffer.putInt(i);

	}

	@Override
	public int getInt(long index) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);
		return bbo.byteBuffer.getInt((int) (index- bbo.arrayOffset()));
	}

	@Override
	public void setInt(long index, int i) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);
		bbo.byteBuffer.putInt((int) (index- bbo.arrayOffset()), i);

	}

	@Override
	public long getLong() throws IOException {
		return getCurrentByteBuffer().byteBuffer.getLong();
	}

	@Override
	public long getLong(long index) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);
		return bbo.byteBuffer.getLong((int) (index - bbo.arrayOffset()));
	}

	@Override
	public void setLong(long index, long l) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);		
		bbo.byteBuffer.putLong((int) (index-bbo.arrayOffset()), l);

	}

	@Override
	public void setLong(long l) throws IOException {
		getCurrentByteBuffer().byteBuffer.putLong(l);

	}

	@Override
	public void writeLong(long l) throws IOException {
		getCurrentByteBuffer().byteBuffer.putLong(l);

	}

	@Override
	public void get(byte[] b) throws IOException {
		getCurrentByteBuffer().byteBuffer.get(b);

	}

	@Override
	public void get(long index, byte[] b) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);				
		bbo.byteBuffer.position((int) (index -bbo.arrayOffset()));
		bbo.byteBuffer.get(b);

	}

	@Override
	public void set(long index, byte[] b) throws IOException {
		final ByteBufferOffset bbo = getCachedByteBuffer(index);				
		bbo.byteBuffer.position((int) (index -bbo.arrayOffset()));
		bbo.byteBuffer.put(b);

	}

	@Override
	public void set(byte[] b) throws IOException {
		getCurrentByteBuffer().byteBuffer.put(b);

	}

	@Override
	public void read(byte[] b) throws IOException {
		getCurrentByteBuffer().byteBuffer.get(b);

	}

	@Override
	public void write(byte[] b) throws IOException {
		getCurrentByteBuffer().byteBuffer.put(b);

	}

	@Override
	public void close() throws IOException {
		fileChannel.close();
		raf.close();
		cachedByteBufferList.clear();
	}

}
