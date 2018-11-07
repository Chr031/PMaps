package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.PMap;
import com.pmaps.pmap.filedriver.FileDriver;

public class PartPairFactory implements PairFactory {

	@Override
	public <K, V> Pair<K, V> newPairForReading(PMap<K, V> pMap, long pairPointer) throws IOException {
		return new PartPair<K, V>(pMap, pairPointer);
	}

	@Override
	public <K, V> Pair<K, V> newPairForWriting(PMap<K, V> pMap, long pairPointer, K key, V value) throws IOException {
		Pair<K, V> newPair = new PartPair<>(pMap, pairPointer, key, value);
		// reserve the space for the header of this PartPair

		return newPair;
	}

	/**
	 * TODO : check synchronization
	 */
	@Override
	public <K, V> void registerFreePairPosition(Pair<K, V> removedPair) throws IOException {
		PartPair<K, V> removedPartPair = (PartPair<K, V>) removedPair;

		final FileDriver headerFileDriver = removedPartPair.getPMap().getIndexFileDriver();
		final FileDriver pairFileDriver = removedPartPair.getPMap().getPairFileDriver();

		long currentFreePairHeader = headerFileDriver.getLong(PMap.POSITION_FREE_PAIR_HEADER);
		long currentFreePairData = headerFileDriver.getLong(PMap.POSITION_FREE_PAIR_DATA);

		// this code is highly dependent of the part pair format !

		// we replace in the removedPartPair the next pair pointer with the
		// current free one :
		pairFileDriver.setLong(removedPartPair.pairPointer, currentFreePairHeader);
		headerFileDriver.setLong(PMap.POSITION_FREE_PAIR_HEADER, removedPartPair.pairPointer);

		// we register in the removedPartPairData the next free pair data
		long firstPartDataPointer = removedPartPair.getPartPointer();
		long lastPartDataPointer = removedPartPair.getPartPairData(removedPartPair.getPartPairDataCount() - 1).getPartPointer();

		headerFileDriver.setLong(PMap.POSITION_FREE_PAIR_DATA, firstPartDataPointer);
		pairFileDriver.setLong(lastPartDataPointer + 4, currentFreePairData);

	}

	@Override
	public <K, V> void poolFreePairPosition(Pair<K, V> newPair) throws IOException {
		PartPair<K, V> newPartPair = (PartPair<K, V>) newPair;

		final FileDriver headerFileDriver = newPartPair.getPMap().getIndexFileDriver();
		final FileDriver pairFileDriver = newPartPair.getPMap().getPairFileDriver();

		final long currentFreePairHeader = headerFileDriver.getLong(PMap.POSITION_FREE_PAIR_HEADER);
		final long currentFreePairData = headerFileDriver.getLong(PMap.POSITION_FREE_PAIR_DATA);

		// this code is highly dependent of the part pair format !

		// assign the header pointer !
		if (currentFreePairHeader != PMap.EOF_POSITION) {
			// assign the header pointer !
			newPartPair.setPairPointer(currentFreePairHeader);
			// set the next free header pointer
			final long nextFreeHeaderPointer = pairFileDriver.getLong(currentFreePairHeader);
			headerFileDriver.setLong(PMap.POSITION_FREE_PAIR_HEADER, nextFreeHeaderPointer);
		} else {
			// size of the file should assume that the header will not be
			// overwritten.
			if (newPartPair.pairPointer + PartPairHeader.BYTE_SIZE > pairFileDriver.length())
				pairFileDriver.setLength(newPartPair.pairPointer + PartPairHeader.BYTE_SIZE);
		}

		// now we add the free PartPairData
		long nextFreePairData = currentFreePairData;
		PartPairData nextFreePartData = null;
		while (nextFreePairData != PMap.EOF_POSITION && nextFreePartData == null) {
			PartPairData partData = new PartPairData(pairFileDriver, nextFreePairData);
			nextFreePartData = newPartPair.addPartData(partData.getPartPointer(), partData.getPartLength());
			nextFreePairData = partData.getNextPartPointer();
		}

		// we save the given information.
		if (nextFreePartData != null) {
			pairFileDriver.setInt(nextFreePartData.getPartPointer(), nextFreePartData.getPartLength());
			headerFileDriver.setLong(PMap.POSITION_FREE_PAIR_DATA, nextFreePartData.getPartPointer());
		} else {
			// nextFreePairData must be equal to EOF
			headerFileDriver.setLong(PMap.POSITION_FREE_PAIR_DATA, nextFreePairData);
		}

	}

}
