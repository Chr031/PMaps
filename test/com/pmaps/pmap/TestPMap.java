package com.pmaps.pmap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;

import com.pmaps.object.SKey;
import com.pmaps.object.SValue;
import com.pmaps.pmap.serializer.JavaSerializer;

public class TestPMap {

	private static final int KEY_LENGHT = 10;
	private static final int VALUE_LENGHT = 100;
	private static final int TABLE_SIZE = 1000;

	private static String[] keys;
	private static String[] values;

	private static final File tempDir = new File("./tmp/");

	// new File(System.getProperty("java.io.tmpdir"));

	@BeforeAll
	public static void initDataTable() {

		Random r = new Random(System.nanoTime());

		keys = new String[TABLE_SIZE];
		values = new String[TABLE_SIZE];

		byte[] bk = new byte[KEY_LENGHT];

		for (int i = 0; i < TABLE_SIZE; i++) {
			r.nextBytes(bk);
			keys[i] = new String(bk) + i;

			byte[] bv = new byte[r.nextInt(VALUE_LENGHT)];
			r.nextBytes(bv);
			values[i] = new String(bv) + "abc " + i;

		}

	}

	private ExecutorService executorService;

	public TestPMap() {
		executorService = Executors.newFixedThreadPool(60);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> pMapConstructors() {
		return Arrays.asList(new Object[][] { { new PMapSimpleConstructor(), true },
				{ new PMapCacheAppendSingleDataFileConstructor(), true },
				{ new PMapCacheCompactSingleDataFileConstructor(), true },
				{ new PMapCacheAppendMultipleDataFileConstructor(), true },
				{ new PMapCacheCompactMultipleDataFileConstructor(), true },

		});
	}

	private static Stream<PMapConstructor> createConstructors() {
		return Stream.of(new PMapSimpleConstructor(), new PMapCacheAppendSingleDataFileConstructor(),
				new PMapCacheCompactSingleDataFileConstructor(), new PMapCacheCompactMultipleDataFileConstructor(),
				new PMapCacheAppendMultipleDataFileConstructor());
	}

	static interface PMapConstructor {
		<K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName) throws IOException;
	}

	static class PMapSimpleConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName)
				throws IOException {
			if (!tempDir.exists()) tempDir.mkdirs();
			return new PMap<K, V>(new File(tempDir, "S-" + pMapName));
			
		}

	}

	static class PMapCacheAppendSingleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName)
				throws IOException {
			return new PMap<K, V>("A-" + pMapName, tempDir, new JavaSerializer(), true, true);
		}

	}

	static class PMapCacheCompactSingleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName)
				throws IOException {
			if (!tempDir.exists()) tempDir.mkdirs();
			return new PMap<K, V>("C-" + pMapName, tempDir, new JavaSerializer(), true, false);
		}

	}

	static class PMapCacheAppendMultipleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName)
				throws IOException {
			if (!tempDir.exists()) tempDir.mkdirs();
			return new PMap<K, V>("AM-" + pMapName, tempDir, new JavaSerializer(), true, true, 50 * 1024 * 1024, 50);
		}

	}

	static class PMapCacheCompactMultipleDataFileConstructor implements PMapConstructor {

		@Override
		public <K extends Serializable, V extends Serializable> PMap<K, V> getInstance(String pMapName)
				throws IOException {
			if (!tempDir.exists()) tempDir.mkdirs();
			return new PMap<K, V>("CM-" + pMapName, tempDir, new JavaSerializer(), true, false, 50 * 1024 * 1024, 50);
		}

	}

	@AfterEach
	public void waitAfterTest() throws InterruptedException {
		// Thread.sleep(3000);
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testDummyZeroKeyDeletion(PMapConstructor constructor) throws IOException {

		PMap<Integer, String> pMap = constructor.getInstance("PMap.dummy.bin");
		pMap.clear();

		String v;

		v = pMap.put(1, "1");
		Assertions.assertEquals(null, v);
		v = pMap.put(2, "2");
		Assertions.assertEquals(null, v);

		System.out.println(pMap.size());
		Assertions.assertEquals(2, pMap.size());

		v = pMap.remove(1);
		Assertions.assertEquals("1", v);
		Assertions.assertEquals(1, pMap.size());

		v = pMap.remove(0);
		Assertions.assertEquals(null, v);
		Assertions.assertEquals(1, pMap.size());

		v = pMap.remove(24);
		Assertions.assertEquals(null, v);
		Assertions.assertEquals(1, pMap.size());

		v = pMap.remove(2);
		Assertions.assertEquals(0, pMap.size());
		Assertions.assertEquals("2", v);
		v = pMap.put(1, "123");
		Assertions.assertEquals(null, v);
		Assertions.assertEquals(1, pMap.size());
		pMap.put(0, "156");
		Assertions.assertEquals(2, pMap.size());
		v = pMap.remove(1);
		Assertions.assertEquals("123", v);
		Assertions.assertEquals(1, pMap.size());
		v = pMap.remove(0);
		Assertions.assertEquals("156", v);
		Assertions.assertEquals(0, pMap.size());
		pMap.remove(24);
		Assertions.assertEquals(0, pMap.size());
		pMap.remove(2);
		Assertions.assertEquals(0, pMap.size());
		pMap.put(3, "12ddd3");
		Assertions.assertEquals(1, pMap.size());
		pMap.put(8, "12ddd3");
		Assertions.assertEquals(2, pMap.size());
		pMap.put(12, "12ddd3");
		Assertions.assertEquals(3, pMap.size());

	}

	/**
	 * test insertion and retrieve Test is during insertion, the good old data is
	 * given back !
	 * 
	 * @throws IOException
	 */
	@ParameterizedTest
	@MethodSource("createConstructors")

	public void testGetAndPut(PMapConstructor constructor) throws IOException {

		Random rand = new Random();

		PMap<String, String> pMap = constructor.getInstance("PMap.bin");
		pMap.clear();

		String valueABC = "EEE" + rand.nextInt();
		System.out.println(pMap.put("ABC", valueABC));

		Assertions.assertEquals(valueABC, pMap.get("ABC"));
		System.out.println(pMap.get("ABC"));

		String valueCDE = "ABD" + rand.nextInt();
		System.out.println(pMap.put("CDE", valueCDE));
		Assertions.assertEquals(valueCDE, pMap.get("CDE"));

		String value156 = "156" + rand.nextInt();
		System.out.println(pMap.put("156", value156));
		Assertions.assertEquals(value156, pMap.get("156"));
		System.out.println(pMap.get("156"));

		String value156_2 = "156" + rand.nextInt();

		String oldVal = pMap.put("156", value156_2);
		System.out.println(oldVal);

		Assertions.assertEquals(value156, oldVal);
		System.out.println(value156_2);

		Assertions.assertEquals(value156_2, pMap.get("156"));

		pMap.close();

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testCloseAndReloadMap(PMapConstructor constructor) throws IOException {
		PMap<String, String> pMap = constructor.getInstance("PMap.Reload.bin");
		pMap.clear();

		for (int i = 0; i < TABLE_SIZE; i++) {
			pMap.put(keys[i], values[i]);
		}
		Assertions.assertEquals(TABLE_SIZE, pMap.size());

		pMap.close();

		PMap<String, String> pMap2 = constructor.getInstance("PMap.Reload.bin");

		for (int i = 0; i < TABLE_SIZE; i++) {
			String value = pMap2.get(keys[i]);
			Assertions.assertEquals(values[i], value);
		}

		Assertions.assertEquals(TABLE_SIZE, pMap2.size());

		pMap2.close();
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testSizeAndRemove(PMapConstructor constructor) throws IOException {

		PMap<String, String> pMap = constructor.getInstance("PMap.SizeAndRemove.bin");
		// new PMap<>(new
		// File(tempDir,
		// "PMap.SizeAndRemove.bin"));
		pMap.clear();
		Assertions.assertEquals(0, pMap.size(), "Size is not null after a clear");

		String k1 = "key1";
		String v1 = "value1";

		Assertions.assertEquals(null, pMap.put(k1, v1), "Value should be null");
		Assertions.assertEquals(1, pMap.size(), "Size must be 1");

		String vRemove = pMap.remove(k1);

		Assertions.assertEquals(v1, vRemove);
		Assertions.assertNull(pMap.get(k1));
		Assertions.assertEquals(0, pMap.size(), "Size must be 0");

		String k2 = "key2";
		String v2 = "value2";

		pMap.put(k1, v1);
		vRemove = pMap.put(k1, v2);
		pMap.put(k2, v2);

		Assertions.assertEquals(v1, vRemove);
		Assertions.assertEquals(2, pMap.size(), "Size must be 2");

		vRemove = pMap.remove(k1);

		Assertions.assertEquals(v2, vRemove);
		Assertions.assertNull(pMap.get(k1));
		Assertions.assertEquals(1, pMap.size(), "Size must be 1");

		pMap.close();
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testRandomGetPutRemoveAndSize(PMapConstructor constructor) throws IOException {
		// final File pMapFile = new File(tempDir, "PMap.Random.bin");
		PMap<String, String> pMap = constructor.getInstance("PMap.Random.bin");
		int size = 0;
		pMap.clear();
		Assertions.assertEquals(size, pMap.size(), "Size is not 0 after a clear");

		for (int i = 0; i < TABLE_SIZE; i++) {

			String vOld = pMap.put(keys[i], values[i]);
			if (vOld == null)
				size++;
			else
				System.out.println("There will be an error !");

			Assertions.assertEquals(size, pMap.size());

		}

		for (int i = 0; i < TABLE_SIZE; i++) {
			String vCurrent = pMap.get(keys[i]);
			Assertions.assertEquals(values[i], vCurrent);
		}

		for (int i = 0; i < TABLE_SIZE; i++) {
			String value = pMap.remove(keys[i]);
			Assertions.assertEquals( values[i], value, "Value does not match for index " + i);
		}

		Assertions.assertEquals(0, pMap.size(), "Size should be equal to 0");

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testRandomSuccessiveGetPutRemoveAndSize(PMapConstructor constructor) throws IOException {
		// final File pMapFile = new File(tempDir, "PMap.Random.bin");
		PMap<String, String> pMap = constructor.getInstance("PMap.Random.Successive.bin");
		// new
		// PMap<>(pMapFile);
		int size = 0;
		pMap.clear();
		Assertions.assertEquals(size, pMap.size(), "Size is not 0 after a clear");

		boolean present[] = new boolean[TABLE_SIZE];
		Arrays.fill(present, false);

		Random r = new Random(System.currentTimeMillis());

		int nbrAction = 10000;
		int cpt = 0;

		while (cpt++ < nbrAction) {
			int index = r.nextInt(TABLE_SIZE);
			if (present[index]) {
				String v = pMap.get(keys[index]);
				Assertions.assertEquals(values[index], v);
				v = pMap.remove(keys[index]);
				Assertions.assertEquals(values[index], v);
				present[index] = false;
			} else {
				String v = pMap.get(keys[index]);
				Assertions.assertNull(v);
				v = pMap.put(keys[index], values[index]);
				Assertions.assertNull(v);
				present[index] = true;
			}
		}

		int calculatedSize = 0;
		for (boolean b : present) {
			if (b)
				calculatedSize++;
		}
		Assertions.assertEquals(calculatedSize, pMap.size(), "Size should be equal to " + calculatedSize);

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testFixPutAndRemove(PMapConstructor constructor) throws IOException {
		PMap<Integer, String> pMap = constructor.getInstance("PMap.fixed.bin");
		// new PMap<>(new File(tempDir, "PMap.fixed.bin"));
		pMap.clear();
		try {

			int[] elementsToAdd = new int[] { 46, 40, 73, 72, 22, 34, 78, 13, 48, 10 };
			int[] elementsToRemove = elementsToAdd;

			/*
			 * int[] elementsToAdd = new int[] { 1, 5, 2, 3, 6, 12 }; int[] elementsToRemove
			 * = new int[] { 1, 5, 3,2, 6, 12 };
			 */

			for (int i : elementsToAdd) {
				pMap.put(i, "" + i);
				System.out.println(pMap.get(i));
			}

			for (int i : elementsToRemove) {

				for (int j : pMap.keySet()) {
					System.out.println(j + " : " + pMap.get(j));
				}

				String v = pMap.remove(i);
				if (v == null)
					for (int j : pMap.keySet()) {
						System.out.println(j + " : " + pMap.get(j));
					}
				Assertions.assertEquals("" + i, v, "Value for " + i + " is not valid !");
			}

		} finally {
			pMap.close();
		}
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testRandomIntPutAndRemove(PMapConstructor constructor) throws IOException {
		PMap<Integer, String> pMap = constructor.getInstance("PMap.randomInt.bin");
		pMap.clear();
		try {
			Random r = new Random();
			int size = 100;
			List<Integer> elements = new ArrayList<Integer>();
			for (int i = 0; i < size; i++) {
				int v;
				do {
					v = r.nextInt(size * size);
				} while (elements.indexOf(v) != -1);
				elements.add(v);

			}
			Integer[] elementsToAdd = elements.toArray(new Integer[] {});
			Integer[] elementsToRemove = elementsToAdd;

			for (int i : elementsToAdd) {
				pMap.put(i, "" + i);
				// System.out.println(pMap.get(i));
			}

			for (int i : elementsToRemove) {

				String v = pMap.remove(i);

				// System.out.println("Remove " + i);
				if (v == null)
					for (int j : pMap.keySet()) {
						// System.out.println(j + " : " + pMap.get(j));
					}
				Assertions.assertEquals("" + i, v, "Value for " + i + " is not valid !");
			}

		} finally {
			pMap.close();
		}
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testFixSuccessivePutAndRemove(PMapConstructor constructor) throws IOException {
		PMap<Integer, String> pMap = constructor.getInstance("PMap.fixed.successive.bin");
		pMap.clear();
		try {

			int[] elementsToAdd = new int[] { 46, 40, 73, 72, 22, 34, 78, 13, 48, 10, 5, 6, 7, 8, 74, 56, 9 };
			boolean[] presence = new boolean[elementsToAdd.length];

			/*
			 * int[] elementsToAdd = new int[] { 1, 5, 2, 3, 6, 12 }; int[] elementsToRemove
			 * = new int[] { 1, 5, 3,2, 6, 12 };
			 */

			for (int i = 0; i < elementsToAdd.length; i++) {
				pMap.put(elementsToAdd[i], "" + elementsToAdd[i]);
				presence[i] = true;
				System.out.println(pMap.get(elementsToAdd[i]));
			}

			Random r = new Random(System.currentTimeMillis());

			int cpt = 0;
			while (cpt++ < 100) {
				int k = r.nextInt(elementsToAdd.length);

				int e = elementsToAdd[k];

				String v = pMap.get(e);
				if (presence[k]) {
					Assertions.assertEquals("" + e, v,
							cpt + " : element " + e + " with index k should be present in the map");
				} else
					Assertions.assertNull(v,
							cpt + " : element " + e + " with index k should not be present in the map");

				if (presence[k]) {
					v = pMap.remove(e);
					Assertions.assertEquals(
							"" + e, v,
							cpt + " : element " + e + " with index k should be present in the map while removing it");
					// System.out.println(e + " removed successfully");
				} else {
					v = pMap.put(e, "" + e);
					Assertions.assertNull(v,
							cpt + " : element " + e + " with index k should not be present in the map, while adding it"
							);
					// System.out.println(e + " added successfully");
				}
				presence[k] = !presence[k];

				for (Entry<Integer, String> entry : pMap.entrySet()) {
					System.out.println(entry.getKey() + " : " + entry.getValue());
				}

			}

		} finally {
			pMap.close();
		}
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testRandomSuccessivePutAndRemove(PMapConstructor constructor) throws IOException {
		PMap<Integer, String> pMap = constructor.getInstance("PMap.random.successive.bin");
		pMap.clear();
		try {

			Set<Integer> elements = new HashSet<Integer>();
			Random r = new Random(System.currentTimeMillis());

			int nbrOfElements = 1000;
			int nbrOfOpperation = 10000;
			int indexInterval = 2000;

			for (int i = 0; i < nbrOfElements; i++) {
				elements.add(r.nextInt(indexInterval));
			}

			for (int e : elements) {
				pMap.put(e, "" + e);
				System.out.println(e);
			}

			int cpt = 0;
			while (cpt++ < nbrOfOpperation) {

				// System.out.print(pMap.size() + "-|");

				boolean add = r.nextInt(3) < 2 || pMap.size() == 0;

				int e = 0;
				String v = null;
				if (add) {
					e = r.nextInt(indexInterval);
					v = pMap.put(e, "" + e);

					// System.out.println(cpt + " : add " + e + " : " + v);

					if (elements.contains(e)) {
						Assertions.assertEquals("" + e, v, e + " should be present");
					} else {
						Assertions.assertNull(v, e + " should not be present ");
					}
					elements.add(e);

				} else {

					int index = r.nextInt(elements.size());
					Iterator<Integer> it = elements.iterator();
					int i = 0;
					while (it.hasNext() && i++ < index) {
						e = it.next();
					}
					v = pMap.remove(e);

					// System.out.println(cpt + " : remove " + e + " : " + v);

					if (elements.contains(e)) {
						Assertions.assertEquals("" + e, v, e + " should be present");
					} else {
						Assertions.assertNull(v, e + " should not be present ");
					}
					elements.remove(e);
				}

				/*
				 * for (Entry<Integer, String> entry : pMap.entrySet()) {
				 * System.out.println(entry.getKey() + " : " + entry.getValue()); }
				 */

			}

		} finally {
			pMap.close();
		}
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testPersistedSize(PMapConstructor constructor) throws IOException {

		PMap<String, String> pMap = constructor.getInstance("PMap.size.bin");
		pMap.clear();

		pMap.put("AA", "AAA");
		Assertions.assertEquals(1, pMap.size());
		pMap.put("BB", "BBB");
		Assertions.assertEquals(2, pMap.size());
		pMap.put("CC", "CCC");
		Assertions.assertEquals(3, pMap.size());

		pMap.put("AB", "AAA");
		Assertions.assertEquals(4, pMap.size());
		pMap.put("BB", "BBBB");
		Assertions.assertEquals(4, pMap.size());
		pMap.put("CD", "CCC");
		Assertions.assertEquals(5, pMap.size());
		;

		Assertions.assertEquals("BBBB", pMap.get("BB"));

		pMap.close();

		pMap = constructor.getInstance("PMap.size.bin");
		Assertions.assertEquals(5, pMap.size(), "Persisted size is not 5 ");

		pMap.close();

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testMultipleKeysAndValues(PMapConstructor constructor) throws IOException {
		PMap<SKey, SValue> pMap = constructor.getInstance("PMap.Skey.bin");
		pMap.clear();
		for (int i = 0; i < 50; i++) {
			SValue val = pMap.put(new SKey("aa", i), new SValue(("aaaa " + i).getBytes()));
			Assertions.assertNull(val);
		}

		SValue val = pMap.get(new SKey("aa", 40));
		System.out.println(new String(val.getContent()));
		Assertions.assertEquals("aaaa 40", new String(val.getContent()));
		pMap.close();

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testReadConcurrent(PMapConstructor constructor)
			throws IOException, InterruptedException, ExecutionException {
		final PMap<String, String> pMap = constructor.getInstance("PMap.ConcurrentRead.bin");
		pMap.clear();

		for (int i = 0; i < TABLE_SIZE; i++) {
			pMap.put(keys[i], values[i]);
		}

		Map<Integer, Future<String>> futureMap = new HashMap<>();

		final Random r = new Random(System.nanoTime());

		for (int i = 0; i < TABLE_SIZE; i++) {
			final int j = r.nextInt(TABLE_SIZE);
			Future<String> future = executorService.submit(new Callable<String>() {

				@Override
				public String call() throws Exception {
					return pMap.get(keys[j]);
				}
			});
			futureMap.put(j, future);
		}

		for (Entry<Integer, Future<String>> futureEntry : futureMap.entrySet()) {
			Assertions.assertEquals(values[futureEntry.getKey()], futureEntry.getValue().get());
		}
	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testIterator(PMapConstructor constructor) throws IOException {

		PMap<String, String> pMap = constructor.getInstance("PMap.Iterator.bin");

		pMap.clear();

		for (int i = 0; i < TABLE_SIZE; i++) {
			pMap.put(keys[i], values[i]);
		}

		Assertions.assertEquals(TABLE_SIZE, pMap.size());

		Set<String> keySet = pMap.keySet();
		Assertions.assertEquals(TABLE_SIZE, keySet.size());
		boolean presence[] = new boolean[TABLE_SIZE];
		Arrays.fill(presence, false);
		for (String key : keySet) {
			String value = pMap.get(key);
			for (int i = 0; i < TABLE_SIZE; i++) {
				if (key.equals(keys[i])) {
					presence[i] = true;
					Assertions.assertEquals(values[i], value);
					break;
				}
			}
		}

		for (int i = 0; i < TABLE_SIZE; i++) {
			if (!presence[i])
				Assertions.fail("Key " + i + " is not present in the keySet");
		}

		String[] keysList = keySet.toArray(new String[] {});
		Assertions.assertEquals(TABLE_SIZE, keys.length);

		Collection<String> valuesCollection = pMap.values();
		Arrays.fill(presence, false);
		for (String value : valuesCollection) {
			boolean exists = false;
			for (int i = 0; i < TABLE_SIZE; i++) {
				exists |= values[i].equals(value);
				if (exists) {
					Assertions.assertFalse(presence[i], "Value + " + i + " should not be present");
					presence[i] = true;
					break;
				}
			}
		}

		for (int i = 0; i < TABLE_SIZE; i++) {
			if (!presence[i])
				Assertions.fail("Value " + i + " is not present in the valuesCollection");
		}

		Assertions.assertEquals(TABLE_SIZE, valuesCollection.size());

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testIteratorConcurrent(PMapConstructor constructor)
			throws IOException, InterruptedException, ExecutionException {
		final PMap<String, String> pMap = constructor.getInstance("PMap.ConcurrentReadIter.bin");

		pMap.clear();

		for (int i = 0; i < TABLE_SIZE; i++) {
			pMap.put(keys[i], values[i]);
		}

		Map<Integer, Future<Boolean>> futureMap = new HashMap<>();

		final long[] totalTime = new long[1];
		long totalSpentTime = System.nanoTime();

		for (int i = 0; i < 20; i++) {

			Future<Boolean> future = executorService.submit(new Callable<Boolean>() {

				@Override
				public Boolean call() throws Exception {
					boolean check = false;
					long start = System.nanoTime();
					for (Entry<String, String> entry : pMap.entrySet()) {
						String key = entry.getKey();
						String value = entry.getValue();

						check = true;

						/*
						 * for (int i = 0; i < TABLE_SIZE; i++) { if (key.equals(keys[i])) {
						 * Assertions.assertEquals(values[i], value); check |= values[i].equals(value);
						 * break; } }
						 */
					}
					totalTime[0] += System.nanoTime() - start;
					return check;
				}
			});
			futureMap.put(i, future);

		}

		for (Entry<Integer, Future<Boolean>> futureEntry : futureMap.entrySet()) {
			Assertions.assertTrue(futureEntry.getValue().get());
		}

		totalSpentTime = System.nanoTime() - totalSpentTime;

		System.out.println("totalTime : " + (totalTime[0] / 1000000d));
		System.out.println("totalSpentTime : " + (totalSpentTime / 1000000d));

		System.out.println("Processor optimisation : " + (totalTime[0] - totalSpentTime) / 1000000d);

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testIteratorConcurrentModificationException(PMapConstructor constructor) throws IOException {

		PMap<Integer, String> pMap = constructor.getInstance("PMap.ConcurrentModificationException.bin");
		pMap.clear();

		pMap.put(1, "A");
		pMap.put(2, "B");
		pMap.put(3, "C");

		Iterator<Integer> iter = pMap.keySet().iterator();

		int key = iter.next();
		System.out.println(key + " : " + pMap.get(key));

		key = iter.next();
		System.out.println(key + " : " + pMap.get(key));

		pMap.put(4, "D");

		iter.next();

		Assertions.fail("No ConcurrentModificationException has been thrown.");
		// exception should be thrown!

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testConcurrentWrite(PMapConstructor constructor)
			throws IOException, InterruptedException, ExecutionException {
		final PMap<String, String> pMap = constructor.getInstance("PMap.ConcurrentWrite.bin");// new
																								// PMap<>(new
																								// File(tempDir,
																								// "PMap.ConcurrentWrite.bin"));
		pMap.clear();

		Map<Integer, Future<Void>> futureMap = new HashMap<>();

		for (int i = 0; i < TABLE_SIZE; i++) {
			final int j = i;
			Future<Void> future = executorService.submit(new Callable<Void>() {

				@Override
				public Void call() throws Exception {
					String value = pMap.put(keys[j], values[j]);
					Assertions.assertNull(value);
					return null;
				}
			});
			futureMap.put(i, future);
		}

		for (Entry<Integer, Future<Void>> futureEntry : futureMap.entrySet()) {
			futureEntry.getValue().get();
		}

		for (int i = 0; i < TABLE_SIZE; i++) {

			Assertions.assertEquals( values[i], pMap.get(keys[i]), i + " value is not the good one : ");

		}

	}

	@ParameterizedTest
	@MethodSource("createConstructors")
	public void testConccurrentReadWriteRemove(PMapConstructor constructor) throws IOException, InterruptedException {

		final PMap<String, String> pMap = constructor.getInstance("PMap.ConcurrentReadWriteRemove.bin");
		pMap.clear();

		List<Future<Integer>> futureList = new ArrayList<>();
		final Random r = new Random(System.currentTimeMillis());
		int cpt = 1000;
		for (int i = 0; i < cpt; i++) {

			Future<Integer> future = executorService.submit(new Callable<Integer>() {

				@Override
				public Integer call() throws Exception {
					int index = r.nextInt(TABLE_SIZE);
					int action = r.nextInt(2);

					// System.out.println(" +++ " + index + " (" + action + ") " +
					// System.nanoTime());

					switch (action) {
					case 0:
						String v = pMap.get(keys[index]);
						// v should be either null either equal to values[index]
						if (v == null) {
							// System.out.println("\t\tnot present : " + index);
						} else {
							Assertions.assertEquals(values[index], v);
							// System.out.println("\t\tpresent : " + index);
						}
						break;
					case 1:
						v = pMap.put(keys[index], values[index]);
						if (v == null) {
							// System.out.println("\t\tInserted : " + index + " not present ");
						} else {
							Assertions.assertEquals(values[index], v);
							// System.out.println("\t\tInserted : " + index + " present ");
						}
						break;
					case 2:
						v = pMap.remove(keys[index]);
						if (v == null) {
							// System.out.println("\t\tRemoved : " + index + " not present ");
						} else {
							Assertions.assertEquals(values[index], v);
							// System.out.println("\t\tRemoved : " + index + " present ");
						}
						break;
					}

					// System.out.println(" --- " + index + " (" + action + ") " +
					// System.nanoTime());

					return index;
				}
			});
			futureList.add(future);
		}

		List<ExecutionException> exceptionList = new ArrayList<>();

		for (Future<Integer> future : futureList) {

			try {
				future.get();
			} catch (ExecutionException e) {
				e.printStackTrace();
				exceptionList.add(e);
			}
		}

		if (exceptionList.size() > 0)
			Assertions.fail("Exception are present : please check ");

	}

}
