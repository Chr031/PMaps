package com.pmaps.pmap.filedriver;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RandomAccessXFile implements Closeable, DataInput, DataOutput, AutoCloseable {

	private class RandomAccessFileOffset {
		private final RandomAccessFile randomAccessFile;
		private final long offset;

		public RandomAccessFileOffset(RandomAccessFile randomAccessFile, long offset) {
			super();
			this.randomAccessFile = randomAccessFile;
			this.offset = offset;
		}

		public long arrayOffset() {
			return offset;
		}

	}

	private final Map<Integer, RandomAccessFileOffset> randomAccessFileMap;
	private volatile RandomAccessFileOffset currentRandomAccessFileOffset;

	private final String baseName;
	private final File baseDirectory;
	private final int maxDataFileSize;
	private final int maxNumberOfDataFile;

	private final Pattern baseNamePattern;

	public RandomAccessXFile(String baseName, File baseDirectory, int maxDataFileSize, int maxNumberOfDataFile) throws IOException {

		this.randomAccessFileMap = new HashMap<>(maxNumberOfDataFile);

		this.baseName = baseName;
		this.baseDirectory = baseDirectory;
		this.maxDataFileSize = maxDataFileSize;
		this.maxNumberOfDataFile = maxNumberOfDataFile;

		baseNamePattern = Pattern.compile(baseName + "\\.([0-9]+)");

		// retrieves the already present files
		File[] dataFiles = baseDirectory.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.isFile() && baseNamePattern.matcher(pathname.getName()).matches();
			}

		});

		for (File dataFile : dataFiles) {
			final Matcher m = baseNamePattern.matcher(dataFile.getName());
			if (m.find()) {
				int fileNumber = Integer.parseInt(m.group(1));
				int offset = fileNumber * maxDataFileSize;
				randomAccessFileMap.put(fileNumber, new RandomAccessFileOffset(new RandomAccessFile(dataFile, "rw"), offset));
			}
		}

		// TODO set the currentRandomAccessFileOffset ! // or create a new one !
		currentRandomAccessFileOffset = getRandomAccessFileOffset(0);
	}

	private File getFileByIndex(int fileIndex) {
		return new File(baseDirectory, baseName + "." + fileIndex);
	}

	private RandomAccessFileOffset getRandomAccessFileOffset(long position) throws IOException {
		if (position < 0)
			throw new IOException("Negative seek not allowed");

		final int index = (int) (position / maxDataFileSize);
		if (index >= maxNumberOfDataFile)
			throw new IOException("Data index is bigger than what is allowed");
		RandomAccessFileOffset rafo = randomAccessFileMap.get(index);
		if (rafo == null) {
			rafo = new RandomAccessFileOffset(new RandomAccessFile(getFileByIndex(index), "rw"), (long) index * maxDataFileSize);
			randomAccessFileMap.put(index, rafo);
		}
		return rafo;
	}

	private RandomAccessFileOffset getCurrentRandomAccessFileOffset() throws IOException {

		if (currentRandomAccessFileOffset.randomAccessFile.getFilePointer() == currentRandomAccessFileOffset.randomAccessFile.length()) {
			seek(currentRandomAccessFileOffset.arrayOffset() + currentRandomAccessFileOffset.randomAccessFile.getFilePointer());
		}

		return currentRandomAccessFileOffset;
	}

	// 'Random access' stuff

	public long length() throws IOException {
		if (randomAccessFileMap.size() == 0)
			return 0;

		long l = (randomAccessFileMap.size() - 1) * maxDataFileSize;

		final RandomAccessFileOffset rafo = randomAccessFileMap.get(randomAccessFileMap.size() - 1);
		if (rafo == null)
			throw new IOException("Data file " + baseName + " with index " + (randomAccessFileMap.size() - 1) + " is not registered.");

		l += rafo.randomAccessFile.length();
		return l;
	}

	public void setLength(long newLength) throws IOException {
		int index = (int) (newLength / maxDataFileSize);
		if (index >= maxNumberOfDataFile)
			throw new IOException("Length is bigger than what is allowed");

		final int currentSize = randomAccessFileMap.size();
		for (int i = index + 1; i < currentSize; i++) {
			final RandomAccessFileOffset rafo = randomAccessFileMap.remove(i);
			if (rafo != null)
				rafo.randomAccessFile.close();
			getFileByIndex(i).delete();
		}

		for (int i = 0; i < index; i++) {
			RandomAccessFileOffset rafo = randomAccessFileMap.get(i);
			if (rafo == null) {
				rafo = new RandomAccessFileOffset(new RandomAccessFile(getFileByIndex(i), "rw"), (long) i * maxDataFileSize);
				randomAccessFileMap.put(i, rafo);
			}
			rafo.randomAccessFile.setLength(maxDataFileSize);

		}

		getRandomAccessFileOffset(newLength).randomAccessFile.setLength(newLength - index * maxDataFileSize);
		// currentRandomAccessFileOffset = getRandomAccessFileOffset(index);

	}

	public void seek(long position) throws IOException {
		final RandomAccessFileOffset rafo = getRandomAccessFileOffset(position);
		rafo.randomAccessFile.seek(position - rafo.arrayOffset());
		currentRandomAccessFileOffset = rafo;

	}

	// 'Read' primitives

	public int read() throws IOException {
		final RandomAccessFileOffset rafo = getCurrentRandomAccessFileOffset();
		return rafo.randomAccessFile.read();
	}

	private int readBytes(byte b[], int off, int len) throws IOException {
		int i = 0;
		while (i < len) {
			final RandomAccessFileOffset rafo = getCurrentRandomAccessFileOffset();
			final int remaining = (int) (rafo.randomAccessFile.length() - rafo.randomAccessFile.getFilePointer());
			int r = +rafo.randomAccessFile.read(b, off + i, Math.min(len - i, remaining));
			if (r == -1)
				return -1;
			else
				i += r;
		}
		return i;

	}

	public int read(byte b[], int off, int len) throws IOException {
		return readBytes(b, off, len);
	}

	public int read(byte b[]) throws IOException {
		return readBytes(b, 0, b.length);
	}

	public final void readFully(byte b[]) throws IOException {
		readFully(b, 0, b.length);
	}

	public final void readFully(byte b[], int off, int len) throws IOException {
		int n = 0;
		do {
			int count = this.read(b, off + n, len - n);
			if (count < 0)
				throw new EOFException();
			n += count;
		} while (n < len);
	}

	// 'Write' primitives

	public void write(int b) throws IOException {
		getCurrentRandomAccessFileOffset().randomAccessFile.write(b);
	}

	private void writeBytes(byte b[], int off, int len) throws IOException {
		int i = 0;
		while (i < len) {
			final RandomAccessFileOffset rafo = getCurrentRandomAccessFileOffset();
			final int remaining = (int) (maxDataFileSize - rafo.randomAccessFile.getFilePointer());
			final int toWrite = Math.min(len - i, remaining);
			rafo.randomAccessFile.write(b, off + i, toWrite);
			i += toWrite;

		}
	}

	public void write(byte b[]) throws IOException {
		writeBytes(b, 0, b.length);
	}

	public void write(byte b[], int off, int len) throws IOException {
		writeBytes(b, off, len);
	}

	public void close() throws IOException {
		for (RandomAccessFileOffset rafo : randomAccessFileMap.values()) {
			rafo.randomAccessFile.close();
		}
	}

	//
	// Some "reading/writing Java data types" methods stolen from
	// DataInputStream and DataOutputStream.
	//

	public final boolean readBoolean() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return (ch != 0);
	}

	public final byte readByte() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return (byte) (ch);
	}

	public final int readUnsignedByte() throws IOException {
		int ch = this.read();
		if (ch < 0)
			throw new EOFException();
		return ch;
	}

	public final short readShort() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (short) ((ch1 << 8) + (ch2 << 0));
	}

	public final int readUnsignedShort() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (ch1 << 8) + (ch2 << 0);
	}

	public final char readChar() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (char) ((ch1 << 8) + (ch2 << 0));
	}

	public final int readInt() throws IOException {
		int ch1 = this.read();
		int ch2 = this.read();
		int ch3 = this.read();
		int ch4 = this.read();
		if ((ch1 | ch2 | ch3 | ch4) < 0)
			throw new EOFException();
		return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
	}

	public final long readLong() throws IOException {
		return ((long) (readInt()) << 32) + (readInt() & 0xFFFFFFFFL);
	}

	public final float readFloat() throws IOException {
		return Float.intBitsToFloat(readInt());
	}

	public final double readDouble() throws IOException {
		return Double.longBitsToDouble(readLong());
	}

	public final String readLine() throws IOException {
		throw new IOException("Not implemented");
	}

	public final String readUTF() throws IOException {
		return DataInputStream.readUTF(this);
	}

	public final void writeBoolean(boolean v) throws IOException {
		write(v ? 1 : 0);
		// written++;
	}

	public final void writeByte(int v) throws IOException {
		write(v);
		// written++;
	}

	public final void writeShort(int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
		// written += 2;
	}

	public final void writeChar(int v) throws IOException {
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
		// written += 2;
	}

	public final void writeInt(int v) throws IOException {
		write((v >>> 24) & 0xFF);
		write((v >>> 16) & 0xFF);
		write((v >>> 8) & 0xFF);
		write((v >>> 0) & 0xFF);
		// written += 4;
	}

	public final void writeLong(long v) throws IOException {
		write((int) (v >>> 56) & 0xFF);
		write((int) (v >>> 48) & 0xFF);
		write((int) (v >>> 40) & 0xFF);
		write((int) (v >>> 32) & 0xFF);
		write((int) (v >>> 24) & 0xFF);
		write((int) (v >>> 16) & 0xFF);
		write((int) (v >>> 8) & 0xFF);
		write((int) (v >>> 0) & 0xFF);
		// written += 8;
	}

	public final void writeFloat(float v) throws IOException {
		writeInt(Float.floatToIntBits(v));
	}

	public final void writeDouble(double v) throws IOException {
		writeLong(Double.doubleToLongBits(v));
	}

	public final void writeBytes(String s) throws IOException {
		int len = s.length();
		byte[] b = new byte[len];
		s.getBytes(0, len, b, 0);
		writeBytes(b, 0, len);
	}

	public final void writeChars(String s) throws IOException {
		int clen = s.length();
		int blen = 2 * clen;
		byte[] b = new byte[blen];
		char[] c = new char[clen];
		s.getChars(0, clen, c, 0);
		for (int i = 0, j = 0; i < clen; i++) {
			b[j++] = (byte) (c[i] >>> 8);
			b[j++] = (byte) (c[i] >>> 0);
		}
		writeBytes(b, 0, blen);
	}

	public final void writeUTF(String str) throws IOException {
		throw new IOException("Not implemented");
		// DataOutputStream.writeUTF(str, this);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		throw new IOException("Not implemented");
	}

}
