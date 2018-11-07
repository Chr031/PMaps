package com.pmaps.pmap.filedriver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class RandomAccessFileDriver implements FileDriver {

	protected final RandomAccessFile raf;
	protected final Object accessLock;
	
	public RandomAccessFileDriver(File file ) throws FileNotFoundException {
		this(file, "rw");
	}

	public RandomAccessFileDriver(File file, String mode) throws FileNotFoundException {
		raf = new RandomAccessFile(file, mode);
		this.accessLock = new Object();
	}

	public Object getAccessLock() {
		return accessLock;
	}

	@Override
	public void seek(long position) throws IOException {
		raf.seek(position);
		
	}

	@Override
	public int getInt() throws IOException {
		return raf.readInt();
	}

	@Override
	public int readInt() throws IOException {
		return raf.readInt();
	}

	@Override
	public void setInt(int i) throws IOException {
		raf.writeInt(i);
		
	}

	@Override
	public int getInt(long index) throws IOException {
		raf.seek(index);
		return raf.readInt();
	}

	@Override
	public void setInt(long index, int i) throws IOException {
		raf.seek(index);
		raf.writeInt(i);
		
	}

	@Override
	public long getLong() throws IOException {
		return raf.readLong();
	}

	@Override
	public long getLong(long index) throws IOException {
		raf.seek(index);
		return raf.readLong();
	}

	@Override
	public void setLong(long index, long l) throws IOException {
		raf.seek(index);
		raf.writeLong(l);
	}

	@Override
	public void setLong(long l) throws IOException {
		raf.writeLong(l);
		
	}

	@Override
	public void setLength(long newLength) throws IOException {
		raf.setLength(newLength);
		
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
		return raf.length();
	}

	@Override
	public void write(byte[] bytes) throws IOException {
		raf.write(bytes);
		
	}

	@Override
	public void get(byte[] b) throws IOException {
		raf.read(b);
		
	}

	@Override
	public void get(long index, byte[] b) throws IOException {
		raf.seek(index);
		raf.read(b);
	}

	@Override
	public void set(long index, byte[] b) throws IOException {
		raf.seek(index);raf.write(b);
		
	}

	@Override
	public void set(byte[] b) throws IOException {
		raf.write(b);
		
	}

	@Override
	public void read(byte[] b) throws IOException {
		raf.read(b);
		
	}

	@Override
	public void close() throws IOException {
		raf.close();
		
	}
	
}
