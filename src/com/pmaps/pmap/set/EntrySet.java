package com.pmaps.pmap.set;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;
import com.pmaps.pmap.index.PairIterator;
import com.pmaps.pmap.pair.Pair;

/**
 * TODO : split into nested classes to have a cleaner code by avoiding
 * anonymous classes (is it really cleaner ?)...
 * 
 * @author Bleu
 * 
 * @param <Kp>
 * @param <Vp>
 */
public class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {

	private final PMap<K,V> pMap;
	
	public EntrySet(PMap<K,V> pMap) {
		this.pMap = pMap;
	}
	
	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		try {
			final PairIterator<K, V> pairIterator = new PairIterator<K, V>(pMap);

			return new Iterator<Map.Entry<K, V>>() {

				@Override
				public boolean hasNext() {
					return pairIterator.hasNext();
				}

				@Override
				public java.util.Map.Entry<K, V> next() {
					final Pair<K, V> pair = pairIterator.next();
					return new Entry<K, V>() {

						@Override
						public K getKey() {
							try {
								return pair.getKey();
							} catch (ClassNotFoundException | IOException e) {
								throw new PMapException("Unable to retrieve the key", e);
							}
						}

						@Override
						public V getValue() {
							try {
								return pair.getValue();
							} catch (ClassNotFoundException | IOException e) {
								throw new PMapException("Unable to retrieve the value", e);
							}
						}

						@Override
						public V setValue(V value) {
							throw new UnsupportedOperationException("Not implemented");
						}
					};
				}

				@Override
				public void remove() {
					throw new UnsupportedOperationException("Not implemented");

				}
			};
		} catch (IOException e) {
			throw new PMapException("Unable to create the iterrator", e);
		}
	}

	@Override
	public int size() {
		return pMap.size();
	}

};