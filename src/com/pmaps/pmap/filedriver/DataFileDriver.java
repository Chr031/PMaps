package com.pmaps.pmap.filedriver;

import java.io.File;
import java.io.IOException;

/**
 * This class maps a list of files with according to (ie baseName + dot +
 * number) and manages them as they where just one file.
 * 
 * @author Bleu
 *
 */
public class DataFileDriver implements FileDriver {

	protected final RandomAccessXFile raxf;
	protected final Object accessLock;
	
	
	public DataFileDriver(String baseName, File baseDirectory, int maxDataFileSize, int maxNumberOfDataFile) throws IOException {
		accessLock = new Object();
		raxf = new RandomAccessXFile(baseName, baseDirectory, maxDataFileSize, maxNumberOfDataFile);
	}

	public Object getAccessLock() {
		return accessLock;
	}

	@Override
	public void seek(long position) throws IOException {
		raxf.seek(position);
		
	}

	@Override
	public int getInt() throws IOException {
		return raxf.readInt();
	}

	@Override
	public int readInt() throws IOException {
		return raxf.readInt();
	}

	@Override
	public void setInt(int i) throws IOException {
		raxf.writeInt(i);
		
	}

	@Override
	public int getInt(long index) throws IOException {
		raxf.seek(index);
		return raxf.readInt();
	}

	@Override
	public void setInt(long index, int i) throws IOException {
		raxf.seek(index);
		raxf.writeInt(i);
		
	}

	@Override
	public long getLong() throws IOException {
		return raxf.readLong();
	}

	@Override
	public long getLong(long index) throws IOException {
		raxf.seek(index);
		return raxf.readLong();
	}

	@Override
	public void setLong(long index, long l) throws IOException {
		raxf.seek(index);
		raxf.writeLong(l);
	}

	@Override
	public void setLong(long l) throws IOException {
		raxf.writeLong(l);
		
	}

	@Override
	public void setLength(long newLength) throws IOException {
		raxf.setLength(newLength);
		
	}

	@Override
	public void writeInt(int i) throws IOException {
		setInt(i);
		
	}

	@Override
	public void writeLong(long l) throws IOException {
		setLong(l);
		
	}

	@Override
	public long length() throws IOException {
		return raxf.length();
	}

	@Override
	public void write(byte[] bytes) throws IOException {
		raxf.write(bytes);
		
	}

	@Override
	public void get(byte[] b) throws IOException {
		raxf.read(b);
		
	}

	@Override
	public void get(long index, byte[] b) throws IOException {
		raxf.seek(index);
		raxf.read(b);
	}

	@Override
	public void set(long index, byte[] b) throws IOException {
		raxf.seek(index);raxf.write(b);
		
	}

	@Override
	public void set(byte[] b) throws IOException {
		raxf.write(b);
		
	}

	@Override
	public void read(byte[] b) throws IOException {
		raxf.read(b);
		
	}

	@Override
	public void close() throws IOException {
		raxf.close();
		
	}
}
