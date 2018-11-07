package com.pmaps.pmap.set;

import java.io.IOException;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Iterator;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.index.PairIterator;
import com.pmaps.pmap.pair.Pair;

class PairSet<K extends Serializable, V extends Serializable> extends AbstractSet<Pair<K, V>> {

	final PMap<K,V> pMap;
	
	
	
	
	public PairSet(PMap<K, V> pMap) {
		super();
		this.pMap = pMap;
	}

	@Override
	public Iterator<Pair<K, V>> iterator() {
		try {
			return new PairIterator<K, V>(pMap);
		} catch (IOException e) {
			throw new RuntimeException("Unable to create the iterator");
		}
	}

	@Override
	public int size() {
		return pMap.size();
	}
}