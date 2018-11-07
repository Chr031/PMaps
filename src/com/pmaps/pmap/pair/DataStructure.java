package com.pmaps.pmap.pair;

import java.io.IOException;

import com.pmaps.pmap.filedriver.FileDriver;

public interface DataStructure {

	void read(FileDriver fileDriver) throws IOException;

	void write(FileDriver fileDriver) throws IOException;

}
