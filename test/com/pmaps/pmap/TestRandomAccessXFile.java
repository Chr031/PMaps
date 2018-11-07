package com.pmaps.pmap;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.pmaps.pmap.filedriver.RandomAccessXFile;

public class TestRandomAccessXFile {

	@Test
	public void testReadData() throws IOException {

		File basePath = new File(".");
		String baseName = "TestXFile.read";

		RandomAccessFile raf1 = new RandomAccessFile(new File(basePath, baseName + ".0"), "rw");
		RandomAccessFile raf2 = new RandomAccessFile(new File(basePath, baseName + ".1"), "rw");

		for (int i = 0; i < 100; i++) {
			RandomAccessFile raf = i < 50 ? raf1 : raf2;
			raf.writeInt(i);
			
		}

		raf1.close();
		raf2.close();

		RandomAccessXFile raxf = new RandomAccessXFile(baseName, basePath, 200, 3);

		for (int i = 0; i < 100; i++) {
			int v = raxf.readInt();
			Assertions.assertEquals( i, v, "");

		}

		raxf.close();
	}

	@Test
	public void testWriteData() throws IOException {

		File basePath = new File(".");
		String baseName = "TestXFile.write";

		RandomAccessXFile raxf = new RandomAccessXFile(baseName, basePath, 200, 5);

		for (int i = 0; i < 200; i++) {
			raxf.writeInt(i * 2);

		}
		raxf.close();

		RandomAccessFile raf1 = new RandomAccessFile(new File(basePath, baseName + ".0"), "rw");
		RandomAccessFile raf2 = new RandomAccessFile(new File(basePath, baseName + ".1"), "rw");
		RandomAccessFile raf3 = new RandomAccessFile(new File(basePath, baseName + ".2"), "rw");
		RandomAccessFile raf4 = new RandomAccessFile(new File(basePath, baseName + ".3"), "rw");

		for (int i = 0; i < 200; i++) {
			RandomAccessFile raf = i < 50 ? raf1 : i < 100 ? raf2 : i < 150 ? raf3 : raf4;
			int v = raf.readInt();
			Assertions.assertEquals(i * 2, v);
		}

		raf1.close();
		raf2.close();
		raf3.close();
		raf4.close();

	}
	
	
	@Test
	public void testLength() throws IOException {
		
		File basePath = new File(".");
		String baseName = "TestXFile.length";
		
		RandomAccessXFile raxf = new RandomAccessXFile(baseName, basePath, 35, 5);
		raxf.setLength(0);
		Assertions.assertEquals(0, raxf.length());
		final byte[] bytes = new byte[]{1,2,3,4,5,6,7,8,9,1,0,2,3,5,4,4,8,9,7,4,5,6,4,1,2,5,5,54,4,45,55};
		
		raxf.write(bytes);
		
		long length = raxf.length();
		Assertions.assertEquals(bytes.length, length);
		
		System.out.println("Length is " + length);
		
		int space = 10;
		
		raxf.setLength(length + space);
		length = raxf.length();
		System.out.println("Length is " + length);
		
		raxf.seek(length);
		
		raxf.write(bytes);
		
		length = raxf.length();
		System.out.println("Length is " + length);
		Assertions.assertEquals(bytes.length *2 + space, length);
		
		raxf.close();
	}
	
	
	
	
	
	@Test 
	public void testWriteByte() throws IOException {
		File basePath = new File(".");
		String baseName = "TestXFile.writeBytes";

		RandomAccessXFile raxf = new RandomAccessXFile(baseName, basePath, 20, 1000);
		raxf.setLength(0);
		//raxf.close();
		
		
		for (int i=0;i<1000;i++) {
			raxf.write(new byte[]{0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
		}
		
		
		int cpt = 10000;
		Random r = new Random(System.currentTimeMillis());
		
		while (cpt -- >0) {
			int p = r.nextInt((16*1000)-300);
			byte b[] = new byte[300];
			raxf.seek(p);
			raxf.read(b);
			Assertions.assertEquals(p%17, b[0]);
			int p2 = r.nextInt(300);
			Assertions.assertEquals((p+p2)%17, b[p2]);
		}
		
		raxf.close();
		
	}
	
	
	
	@Test 
	public void testBigSize() throws IOException {
		File basePath = new File(".");
		String baseName = "TestXFile.writeBigSize";

		final int samplesize = 25*1024*1024;
		final int maxFileSize = 50 * samplesize;
		RandomAccessXFile raxf = new RandomAccessXFile(baseName, basePath, maxFileSize-15647, 100);
		raxf.setLength(0);
		//raxf.close();
		int nbr = 100;
		byte[] bM = new byte[nbr];
		Random r = new Random(System.currentTimeMillis());
		
		
		
		for (int i=0;i<nbr;i++) {
			byte[] b = new byte[samplesize];
		
			r.nextBytes(b);

			
			bM[i] = b[0];
			raxf.write(b);
			
			System.out.println(i + " : " + samplesize + " bytes written");
		}
		
		
		int cpt = 10000;
		
		
		while (cpt -- >0) {
			int p = r.nextInt(nbr  );
			byte b[] = new byte[300];
			raxf.seek(((long)p)*samplesize);
			raxf.read(b);
			Assertions.assertEquals(bM[(int)p], b[0]);
			System.out.println(b[0]);
		}
		
		raxf.close();
		
	}

}
