package com.pmaps.pmap.set;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.index.PairIterator;

class KeyIterator<E> implements Iterator<E> {

	
	final PMap<E,?> pMap;
	final PairIterator<E, ?> pairIterator;

	KeyIterator(PMap<E,?> pMap) throws IOException {
		this.pMap = pMap;
		pairIterator = new PairIterator<>(pMap);
	}

	@Override
	public boolean hasNext() {
		return pairIterator.hasNext();
	}

	@Override
	public E next() {
		try {
			return pairIterator.next().getKey();
		} catch (ClassNotFoundException | IOException e) {
			throw new NoSuchElementException(e.getMessage());
		}
	}

	@Override
	public void remove() {
		pairIterator.remove();

	}

}
