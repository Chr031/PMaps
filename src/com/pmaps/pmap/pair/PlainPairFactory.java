package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.PMap;

public class PlainPairFactory implements PairFactory {

	@Override
	public <K, V> Pair<K, V> newPairForReading(PMap<K, V> pMap, long pairPointer) throws IOException {
		return new PlainPair<K, V>(pMap, pairPointer);
	}

	@Override
	public <K, V> Pair<K, V> newPairForWriting(PMap<K, V> pMap, long pairPointer, K key, V value) {
		return new PlainPair<K, V>(pMap, pairPointer, key, value);
	}

	@Override
	public <K, V> void registerFreePairPosition(Pair<K, V> removedPair) {
		// Blank method : not in the scope of plain pairs

	}

	@Override
	public <K, V> void poolFreePairPosition(Pair<K, V> newPair) {
		// Blank method : not in the scope of plain pairs

	}

}
