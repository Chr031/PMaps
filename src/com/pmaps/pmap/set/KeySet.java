package com.pmaps.pmap.set;

import java.io.IOException;
import java.util.AbstractSet;
import java.util.Iterator;

import com.pmaps.PMapException;
import com.pmaps.pmap.PMap;

public class KeySet<K> extends AbstractSet<K> {

	/**
	 * 
	 */
	private final PMap<K, ?> pMap;

	/**
	 * @param pMap
	 */
	public KeySet(PMap<K, ?> pMap) {
		this.pMap = pMap;
	}

	@Override
	public Iterator<K> iterator() {
		try {
			return new KeyIterator<K>(pMap);
		} catch (IOException e) {
			throw new PMapException("Unable to create the iterator", e);
		}
	}

	@Override
	public int size() {
		return pMap.size();
	}

}