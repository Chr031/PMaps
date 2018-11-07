package com.pmaps.pmap;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.MappedByteBuffer;
import java.util.Collection;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.pmaps.PMapException;
import com.pmaps.pmap.filedriver.DataFileDriver;
import com.pmaps.pmap.filedriver.FileDriver;
import com.pmaps.pmap.filedriver.RandomAccessCachedFileDriver;
import com.pmaps.pmap.filedriver.RandomAccessFileDriver;
import com.pmaps.pmap.index.BTreeNode;
import com.pmaps.pmap.index.BTreeNodeDriver;
import com.pmaps.pmap.index.PairIterator;
import com.pmaps.pmap.pair.Pair;
import com.pmaps.pmap.pair.PairDriver;
import com.pmaps.pmap.pair.PairFactory;
import com.pmaps.pmap.pair.PartPairFactory;
import com.pmaps.pmap.pair.PlainPairFactory;
import com.pmaps.pmap.pair.PairDriver.PairTuple;
import com.pmaps.pmap.serializer.ISerializer;
import com.pmaps.pmap.serializer.JavaSerializer;
import com.pmaps.pmap.set.EntrySet;
import com.pmaps.pmap.set.KeySet;
import com.pmaps.pmap.set.ValueSet;

/**
 * <p>
 * Implementation of a map saving data into a file. Data are organized and
 * indexed using a B-Tree. Access to the data is done in O(log(n)) (read and
 * write)
 * 
 * </p>
 * <p>
 * Implementation is not thread safe. Any multi-threaded access to this file
 * will have impact on data and may damage the underlying data file.
 * <ul>
 * <li>Concurrent read : done</li>
 * <li>TODO Concurrent write</li>
 * </ul>
 * </p>
 * 
 * Keys and Values have to implement the {@link Serializable} interface. Keys
 * and values are stored and accessed against {@link Object#hashCode()} and
 * {@link Object#equals(Object)}. Do not hesitate to overwrite them with your
 * custom implementation.
 * 
 * <ul>
 * Average performance time
 * <li>put : 0.5ms</li>
 * <li>get : 0.3ms</li>
 * </ul>
 * 
 * Remove method is ready
 * 
 * 
 * @author bleu
 * 
 * @param <K>
 * @param <V>
 */
public class PMap<K, V> implements Map<K, V> {

	public static final long POSITION_TOP_NODE = 0;
	public static final int POSITION_SIZE = 8;
	public static final long POSITION_FREE_NODE = 12;
	public static final long POSITION_FREE_PAIR_HEADER = 20;
	public static final long POSITION_FREE_PAIR_DATA = 28;
	public static final long POSITION_FREE_LONG_VALUE = 36;
	public static final long POSITION_PMAP_SIGNATURE = 44;
	public static final long POSITION_START_DATA = 52;

	public static final long PMAP_SIGNATURE = 123456789l;
	public static final long EOF_POSITION = -1;

	private final FileDriver indexFileDriver;
	private final FileDriver pairFileDriver;

	private final BTreeNodeDriver<K, V> nodeDriver;
	private final PairDriver<K, V> pairDriver;
	private final ISerializer serializer;

	private int size;

	private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	private final WeakHashMap<PairIterator<K, V>, Void> weakIterratorMap = new WeakHashMap<>();

	/**
	 * Creates an instance of PMap based on the given file. Index and data will
	 * be stored there. Default {@link JavaSerializer} class (based on the java
	 * native serialization) will be used.
	 * 
	 * @param file
	 *            a plain file where the indexes and data are stored.
	 * @throws IOException
	 */
	public PMap(File file) throws IOException {
		this(file, new JavaSerializer());
	}

	/**
	 * Creates an instance of PMap based on the given file f and on the given
	 * Serializer. Index and data will be stored in the file.
	 * 
	 * @param file
	 *            a plain file where the indexes and data are stored.
	 * @param serializer
	 *            a class managing the conversion between key and values to
	 *            bytes arrays.
	 * @throws IOException
	 * @throws PMapException
	 */
	public PMap(File file, ISerializer serializer) throws PMapException, IOException {
		if (file.isDirectory())
			throw new PMapException(file.getAbsolutePath() + " is a directory");
		if (!file.exists()) {
			file.createNewFile();
			init(file);
		}
		this.indexFileDriver = new RandomAccessFileDriver(file);
		this.pairFileDriver = indexFileDriver;
		boolean correct = checkFileSignature(indexFileDriver);
		if (!correct)
			throw new PMapException("Header signature has not been found. The file may be corrupted");
		initSize();
		nodeDriver = new BTreeNodeDriver<>(this, getIndexFileDriver());
		pairDriver = new PairDriver<>(this, getPairFileDriver(), new PlainPairFactory());
		this.serializer = serializer;
	}
	
	
	/**
	 * <p>
	 * Creates a PMap instance based on two files.
	 * </p>
	 * <p>
	 * One file for the index and one file for the data. Both are located in the
	 * base directory and are named with the mapName argument. Index file is
	 * suffixed with .idx, data file (or values file) is suffixed with .data.
	 * </p>
	 * <p>
	 * The cacheIndex argument, when set to true will create a
	 * {@link MappedByteBuffer} on the index file to allow a fast access and
	 * navigation into it. Caching the index will improve performances for a
	 * relative low memory impact. 1 million of values should use something like
	 * 20 to 40 MB of memory.
	 * 
	 * </p>
	 * 
	 * 
	 * @param mapName
	 * @param baseDirectory
	 * @param serializer
	 * @param cacheIndex
	 * @param appendData
	 * @throws PMapException
	 * @throws IOException
	 */
	public PMap(String mapName, File baseDirectory, ISerializer serializer, boolean cacheIndex, boolean appendData)
			throws PMapException, IOException {
		// Some checks
		if (!baseDirectory.isDirectory())
			throw new PMapException(baseDirectory.getAbsolutePath() + " is not a directory");
		File indexFile = new File(baseDirectory, mapName + ".idx");
		if (!indexFile.exists()) {
			indexFile.createNewFile();
			init(indexFile);
		}

		// set the file driver instances
		this.indexFileDriver = cacheIndex ? new RandomAccessCachedFileDriver(indexFile) : new RandomAccessFileDriver(indexFile);
		File dataFile = new File(baseDirectory, mapName + ".data");
		this.pairFileDriver = new RandomAccessFileDriver(dataFile);

		// init the files and data.
		boolean correct = checkFileSignature(indexFileDriver);
		if (!correct)
			throw new PMapException("Header signature has not been found. The file may be corrupted");
		initSize();
		nodeDriver = new BTreeNodeDriver<>(this, indexFileDriver);
		pairDriver = new PairDriver<>(this, pairFileDriver, appendData ? new PlainPairFactory() : new PartPairFactory());
		this.serializer = serializer;

	}

	public PMap(String mapName, File baseDirectory, ISerializer serializer, boolean cacheIndex, boolean appendData, int maxDataFileSize,
			int maxNumberOfDataFile) throws PMapException, IOException {
		// Some checks
		if (!baseDirectory.isDirectory())
			throw new PMapException(baseDirectory.getAbsolutePath() + " is not a directory");
		File indexFile = new File(baseDirectory, mapName + ".idx");
		if (!indexFile.exists()) {
			indexFile.createNewFile();
			init(indexFile);
		}
		// set the file driver instances
		this.indexFileDriver = cacheIndex ? new RandomAccessCachedFileDriver(indexFile) : new RandomAccessFileDriver(indexFile);
		this.pairFileDriver = new DataFileDriver(mapName, baseDirectory, maxDataFileSize, maxNumberOfDataFile);

		// init the files and data.
		boolean correct = checkFileSignature(indexFileDriver);
		if (!correct)
			throw new PMapException("Header signature has not been found. The file may be corrupted");
		initSize();
		nodeDriver = new BTreeNodeDriver<>(this, indexFileDriver);
		pairDriver = new PairDriver<>(this, pairFileDriver, appendData ? new PlainPairFactory() : new PartPairFactory());
		this.serializer = serializer;

	}

	/**
	 * Protected constructor made for more code flexibility.
	 * 
	 * @param serializer
	 * @param indexFileDriver
	 * @param pairFileDriver
	 * @param pairFactory
	 * @throws IOException
	 */
	protected PMap(ISerializer serializer, FileDriver indexFileDriver, FileDriver pairFileDriver, PairFactory pairFactory)
			throws IOException {
		this.serializer = serializer;
		this.indexFileDriver = indexFileDriver;
		this.pairFileDriver = pairFileDriver;
		this.nodeDriver = new BTreeNodeDriver<>(this, indexFileDriver);
		this.pairDriver = new PairDriver<>(this, pairFileDriver, pairFactory);

		boolean correct = checkFileSignature(indexFileDriver);
		if (!correct)
			throw new PMapException("Header signature has not been found. The file may be corrupted");
		initSize();
	}

	/**
	 * Initialize the file. First 8 bytes contain the position of the root
	 * {@link BTreeNode} in the file. Then next 4 bytes will contain the size of
	 * this map, ie the number of element that are present inside. Then a
	 * default {@link BTreeNode} is created a position 12.
	 * 
	 * @throws IOException
	 */
	private void init(File file) throws IOException {
		FileDriver rafTmp = new RandomAccessFileDriver(file, "rws");
		initFile(rafTmp);
		rafTmp.close();
	}

	/**
	 * Initialize the file. First 8 bytes contain the position of the root
	 * {@link BTreeNode} in the file. Then next 4 bytes will contain the size of
	 * this map, ie the number of element that are present inside. Then a
	 * default {@link BTreeNode} is created a position 12. *
	 * 
	 * @param fileDriver
	 * @throws IOException
	 */
	private void initFile(FileDriver fileDriver) throws IOException {

		synchronized (fileDriver.getAccessLock()) {
			// set the length of the file to be null
			fileDriver.setLength(0);

			// write in the first 8 bytes the position of the top BTreeNode.
			fileDriver.setLong(POSITION_TOP_NODE, EOF_POSITION);
			// 4 next bytes : the size of the map.
			fileDriver.setInt(POSITION_SIZE, 0);
			size = 0;

			// 8 next bytes : the first free position for a node :
			fileDriver.setLong(POSITION_FREE_NODE, EOF_POSITION);

			// 8 next bytes : the first free pair position.
			fileDriver.setLong(POSITION_FREE_PAIR_HEADER, EOF_POSITION);

			// 8 next bytes : the first free pair position.
			fileDriver.setLong(POSITION_FREE_PAIR_DATA, EOF_POSITION);

			// 8 next bytes : Free position used byte index when cached to
			// retrieves it's size :
			fileDriver.setLong(POSITION_FREE_LONG_VALUE, POSITION_START_DATA);

			// 8 next bytes : the pmap signature :
			fileDriver.setLong(POSITION_PMAP_SIGNATURE, PMAP_SIGNATURE);

		}

	}

	/**
	 * This method check if the file header is present and compatible with the
	 * PMap format.
	 * 
	 * @param fileDriver
	 * @throws IOException
	 */
	private boolean checkFileSignature(FileDriver fileDriver) throws IOException {
		long signature = fileDriver.getLong(POSITION_PMAP_SIGNATURE);
		return signature == PMAP_SIGNATURE;
	}

	/**
	 * Read the size of this map from the file and set the #size variable with
	 * the read value.
	 * 
	 * @throws IOException
	 */
	private void initSize() throws IOException {
		synchronized (getIndexFileDriver().getAccessLock()) {
			size = getIndexFileDriver().getInt(POSITION_SIZE);
		}

	}

	/**
	 * Insert the size into the file. It's necessary for persistence;
	 * 
	 * @param newSize
	 * @throws IOException
	 */
	private void setSize(int newSize) throws IOException {
		synchronized (getIndexFileDriver().getAccessLock()) {
			size = newSize;
			getIndexFileDriver().setInt(POSITION_SIZE, newSize);

		}

	}

	protected void finalize() throws Throwable {
		close();
	}

	protected void revokeOngoingIterators() {
		for (PairIterator<K, V> pairIterator : weakIterratorMap.keySet()) {
			pairIterator.onConncurrentModification();
		}
	}

	public void registerOngoingIterator(PairIterator<K, V> iterator) {
		weakIterratorMap.put(iterator, null);
	}

	public Lock getReadLock() {
		return readWriteLock.readLock();
	}

	public FileDriver getIndexFileDriver() {
		return indexFileDriver;
	}

	public FileDriver getPairFileDriver() {
		return pairFileDriver;
	}

	public BTreeNodeDriver<K, V> getNodeDriver() {
		return nodeDriver;
	}

	public PairDriver<K, V> getPairDriver() {
		return pairDriver;
	}

	public ISerializer getSerializer() {
		return serializer;
	}

	public void close() throws IOException {
		getIndexFileDriver().close();
		getPairFileDriver().close();

	}

	/**
	 * Invokes {@link #init()} method. All underlying files used for the storage
	 * are truncated and reinitialized. All data are deleted.
	 */
	@Override
	public void clear() {
		try {
			initFile(getIndexFileDriver());
			if (getIndexFileDriver() != getPairFileDriver())
				getPairFileDriver().setLength(0);
		} catch (IOException e) {
			throw new PMapException("Unable to clear the persisted file", e);
		}

	}

	@Override
	public V get(Object key) {

		final int hashCode = key.hashCode();

		final Lock readLock = readWriteLock.readLock();
		try {
			readLock.lockInterruptibly();
			try {
				BTreeNode<K, V> index = getNodeDriver().goToIndex(hashCode);

				if (index == null)
					return null;

				Enumeration<Pair<K, V>> pairEnum = index.getPairEnumeration(hashCode);

				while (pairEnum.hasMoreElements()) {
					Pair<K, V> pair = pairEnum.nextElement();
					try {
						if (pair.getKey().equals(key))
							return pair.getValue();
					} catch (IOException | ClassNotFoundException e) {
						throw new PMapException("Unable to read data.", e);
					}
				}
			} finally {
				readLock.unlock();
			}
		} catch (InterruptedException ie) {
			throw new PMapException("Reading data interrupted", ie);
		}

		return null;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Insert a key
	 */
	@Override
	public V put(K key, V value) {
		int hashCode = key.hashCode();
		try {

			Lock writeLock = readWriteLock.writeLock();
			writeLock.lockInterruptibly();
			try {
				// look for the tree path :
				LinkedList<Long> treePathPositions = getNodeDriver().findEntryPositions(hashCode);

				BTreeNode<K, V> current = null;
				long lastNodePosition = treePathPositions.pollLast();
				if (lastNodePosition == EOF_POSITION) {

					// new entry must be registered
					current = getNodeDriver().registerNewEntry(treePathPositions, hashCode, EOF_POSITION, null, null);

				} else {
					// entry exits ... we insert the value!
					current = new BTreeNode<>(this, lastNodePosition);
				}

				// look for the pair value :
				// TODO Synchronize the access on the pairFileDriver lock !!!
				Pair<K, V> oldPair = getPairDriver().addPair(current, hashCode, key, value);

				if (oldPair != null)
					return oldPair.getValue();
				else {
					// we increment the size by one :
					setSize(size() + 1);
				}
				revokeOngoingIterators();
			} finally {
				writeLock.unlock();
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new PMapException("Unable to write data", e);
		} catch (InterruptedException ie) {
			throw new PMapException("Write interrupted", ie);
		}
		return null;

	}

	@Override
	public void putAll(Map<? extends K, ? extends V> map) {
		for (Entry<? extends K, ? extends V> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}

	}

	/**
	 * Done : implement index cleanup if no values are present ... Index node is
	 * removed if empty. <b> But this leaves the index unbalanced !</b> Must be
	 * synchronized
	 */
	@Override
	public V remove(Object key) {
		final int hashCode = key.hashCode();
		try {

			final Lock writeLock = readWriteLock.writeLock();
			writeLock.lockInterruptibly();
			try {
				LinkedList<Long> treePathPositions = getNodeDriver().findEntryPositions(hashCode);
				final long indexPosition = treePathPositions.getLast();

				if (indexPosition == EOF_POSITION) {
					// no entry found
					return null;
				}

				BTreeNode<K, V> current = new BTreeNode<K, V>(this, indexPosition);

				PairTuple<K, V> oldPairTuple = pairDriver.removePair(current, hashCode, key);

				if (oldPairTuple == null)
					return null;
				if (oldPairTuple.pairLessNode) {
					// hash entry doesn't reference any values anymore. --> it
					// should be removed
					getNodeDriver().removeHashEntry(treePathPositions, hashCode);
				}

				setSize(size() - 1);
				revokeOngoingIterators();
				return oldPairTuple.pair.getValue();

			} finally {
				writeLock.unlock();
			}
		} catch (IOException | ClassNotFoundException e) {
			throw new PMapException("Unable to remove data.", e);
		} catch (InterruptedException ie) {
			throw new PMapException("Remove interrupted", ie);
		}

	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean containsKey(Object arg0) {
		return get(arg0) != null;
	}

	@Override
	public Set<K> keySet() {
		return new KeySet<K>(this);
	}

	@Override
	public Set<java.util.Map.Entry<K, V>> entrySet() {
		return new EntrySet<K, V>(this);
	}

	@Override
	public Collection<V> values() {
		return new ValueSet<V>(this);
	}

	/**
	 * This method is quite problematic because it will iterate over all the
	 * values present in this persisted map. This may be very slow and
	 * inefficient.
	 * 
	 * (non-Javadoc)
	 * 
	 * @see java.util.Map#containsValue(java.lang.Object)
	 */
	@Override
	public boolean containsValue(Object value) {
		try {
			PairIterator<K, V> pairIterator = new PairIterator<>(this);
			while (pairIterator.hasNext()) {
				Pair<K, V> pair = pairIterator.next();
				if (pair.getValue().equals(value))
					return true;
			}

		} catch (IOException | ClassNotFoundException e) {
			throw new PMapException("Unable to reun throw the iterrator", e);
		}
		return false;
	}

	public long fileSize() throws IOException {
		if (indexFileDriver == pairFileDriver)
			return indexFileDriver.length();
		else
			return indexFileDriver.length() + pairFileDriver.length();
	}

}
