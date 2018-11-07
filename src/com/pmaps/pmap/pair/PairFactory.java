package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.PMap;

public interface PairFactory {

	<K, V> Pair<K, V> newPairForReading(PMap<K, V> pMap, long pairPointer) throws IOException;

	<K, V> Pair<K, V> newPairForWriting(PMap<K, V> pMap, long pairPointer, K key, V value) throws IOException;

	<K, V> void registerFreePairPosition(Pair<K, V> removedPair) throws IOException;

	<K, V> void poolFreePairPosition(Pair<K, V> newPair) throws IOException;

}
