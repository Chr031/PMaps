package com.pmaps.pmap.set;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.index.PairIterator;

class ValueIterator<E> implements Iterator<E> {

	
	final PairIterator<?, E> pairIterator;

	ValueIterator(PMap<?,E> pMap) throws IOException {
		pairIterator = new PairIterator<>(pMap);
		
	}

	@Override
	public boolean hasNext() {
		return pairIterator.hasNext();
	}

	@Override
	public E next() {
		try {
			return pairIterator.next().getValue();
		} catch (ClassNotFoundException | IOException e) {
			throw new NoSuchElementException(e.getMessage());
		}
	}

	@Override
	public void remove() {
		pairIterator.remove();

	}

}
