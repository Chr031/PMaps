package com.pmaps.pmap.filedriver;

import java.io.IOException;

public interface FileDriver {

	Object getAccessLock();

	/**
	 * Returns the sum of the file's lengths of all the files represented by this {@link FileDriver}.
	 * @return 
	 * @throws IOException
	 */
	long length() throws IOException;

	void setLength(long newLength) throws IOException;

	void seek(long position) throws IOException;

	int getInt() throws IOException;

	int readInt() throws IOException;

	void setInt(int i) throws IOException;

	void writeInt(int i) throws IOException;

	int getInt(long index) throws IOException;

	void setInt(long index, int i) throws IOException;

	long getLong() throws IOException;

	long getLong(long index) throws IOException;

	void setLong(long index, long l) throws IOException;

	void setLong(long l) throws IOException;

	void writeLong(long l) throws IOException;

	void get(byte[] b) throws IOException;

	void get(long index, byte[] b) throws IOException;

	void set(long index, byte[] b) throws IOException;

	void set(byte[] b) throws IOException;

	void read(byte[] b) throws IOException;

	void write(byte[] b) throws IOException;

	void close() throws IOException;

}
