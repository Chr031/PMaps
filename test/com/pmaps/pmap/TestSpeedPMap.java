package com.pmaps.pmap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.pmaps.pmap.serializer.JavaSerializer;

@RunWith(Parameterized.class)
public class TestSpeedPMap {

	private static final File tempDir = new File(".");// new
														// File(System.getProperty("java.io.tmpdir"));

	static interface PMapConstructor {
		<K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException;
	}

	static class PMapSimpleConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException {
			return new PMap<K, V>(new File(tempDir, "S-" + pMapName));
		}

	}

	static class PMapCacheAppendSingleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException {
			return new PMap<K, V>("A-" + pMapName, tempDir, new JavaSerializer(), true, true);
		}

	}

	static class PMapCacheCompactSingleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException {
			return new PMap<K, V>("C-" + pMapName, tempDir, new JavaSerializer(), true, false);
		}

	}

	static class PMapCacheAppendMultipleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException {
			return new PMap<K, V>("AM-" + pMapName, tempDir, new JavaSerializer(), true, true, 5 * 1024 *1024 , 5000);
		}

	}

	static class PMapCacheCompactMultipleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException {
			return new PMap<K, V>("CM-" + pMapName, tempDir, new JavaSerializer(), true, false, 5 * 1024 * 1024, 50);
		}

	}

	public final PMapConstructor constructor;
	public final int nbr;
	public final int valueSize;

	public TestSpeedPMap(PMapConstructor constructor, int nbr, int valueSize) {
		this.constructor = constructor;
		this.nbr = nbr;
		this.valueSize = valueSize;
	}

	@Parameterized.Parameters
	public static Collection pMapConstructors() {
		return Arrays.asList(new Object[][] { 
				{ new PMapSimpleConstructor(), 1000,100 }, 
				{ new PMapSimpleConstructor(), 5000,100 },

				{ new PMapCacheAppendSingleDataFileConstructor(), 1000,100 },
				{ new PMapCacheAppendSingleDataFileConstructor(), 10000,100 },
				{ new PMapCacheAppendSingleDataFileConstructor(), 100000,100 },
				{ new PMapCacheAppendSingleDataFileConstructor(), 1000000,100 } ,

				{ new PMapCacheCompactSingleDataFileConstructor(), 1000,100 }, 
				{ new PMapCacheCompactSingleDataFileConstructor(), 10000,100 },
				{ new PMapCacheCompactSingleDataFileConstructor(), 100000,100 },

				{ new PMapCacheAppendMultipleDataFileConstructor(), 1000,100 },
				{ new PMapCacheAppendMultipleDataFileConstructor(), 10000,100 },
				{ new PMapCacheAppendMultipleDataFileConstructor(), 100000,100 },
				{ new PMapCacheAppendMultipleDataFileConstructor(), 100000,100 },
				

				{ new PMapCacheCompactMultipleDataFileConstructor(), 1000,100 }, 
				{ new PMapCacheCompactMultipleDataFileConstructor(), 10000,100 },				
				{ new PMapCacheCompactMultipleDataFileConstructor(), 100000,100 }

		});

	}

	@Test
	public void bulkOrderedInsertion() throws IOException {

		System.out.println("\n\n******************\nOrdered " + nbr + " with " + constructor.getClass().getSimpleName());
		PMap<String, String> pMap = constructor.getInstance(String.format("BTreeMapOrdered.%s.bin", nbr));

		pMap.clear();

		long start = System.nanoTime();

		Random r = new Random(System.currentTimeMillis());

		for (int i = 0; i < nbr; i++) {
			byte[] b = new byte[valueSize];
			r.nextBytes(b);
			pMap.put(" " + i, i + " " + new String(b));

		}

		long time = System.nanoTime() - start;
		double insertionTimeInMillis = time / 1000000d;
		double insertionUnitInsertionTime = insertionTimeInMillis / nbr;
		System.out.println(nbr + " Insertion time (ms) : " + insertionTimeInMillis);
		System.out.println("Unit insertion time (ms) : " + insertionUnitInsertionTime);

		System.out.println("Size of the file (in bytes) : " + pMap.fileSize());
		Pattern p = Pattern.compile("^[0-9]+ ");

		start = System.nanoTime();
		for (int i = 0; i < nbr; i++) {

			String value = pMap.get(" " + i);
			if (!value.startsWith(i + " "))
				Assertions.fail("Value is " + value.substring(0, 20) + " but should start with " + i);

		}

		time = System.nanoTime() - start;
		double readTimeInMillis = time / 1000000d;
		double readUnitInsertionTime = readTimeInMillis / nbr;
		System.out.println(nbr + " read time (ms) : " + readTimeInMillis);
		System.out.println("Unit read time (ms) : " + readUnitInsertionTime);

		pMap.close();
	}

	@Test
	public void bulkRandomInsertion() throws IOException {

		System.out.println("\n\n******************\nRandom " + nbr + " with " + constructor.getClass().getSimpleName());
		// init the random values
		Random r = new Random();
		String[] keys = new String[nbr];
		String[] values = new String[nbr];
		byte[] b = new byte[valueSize];
		for (int i = 0; i < nbr; i++) {
			r.nextBytes(b);
			keys[i] = new String(b);
			r.nextBytes(b);
			values[i] = new String(b);

		}

		PMap<String, String> pMap = constructor.getInstance(String.format("BTreeMapRandom.%s.bin", nbr));

		pMap.clear();

		long start = System.nanoTime();

		for (int i = 0; i < nbr; i++) {
			// System.out.println(keys[i].hashCode());
			pMap.put(keys[i], values[i]);
			// if (i%10==0) System.out.println(i);
		}

		long time = System.nanoTime() - start;
		double insertionTimeInMillis = time / 1000000d;
		double insertionUnitInsertionTime = insertionTimeInMillis / nbr;
		System.out.println(nbr + " Insertion time (ms) : " + insertionTimeInMillis);
		System.out.println("Unit insertion time (ms) : " + insertionUnitInsertionTime);

		System.out.println("Size of the file (in bytes) : " + pMap.fileSize());

		start = System.nanoTime();
		for (int i = 0; i < nbr; i++) {

			String value = pMap.get(keys[i]);
			// System.out.println(i);
			Assertions.assertEquals("get is wrong", values[i], value);
			// System.out.println(value);
		}

		time = System.nanoTime() - start;
		double readTimeInMillis = time / 1000000d;
		double readUnitInsertionTime = readTimeInMillis / nbr;
		System.out.println(nbr + " read time (ms) : " + readTimeInMillis);
		System.out.println("Unit read time (ms) : " + readUnitInsertionTime);

		pMap.close();
	}
}
