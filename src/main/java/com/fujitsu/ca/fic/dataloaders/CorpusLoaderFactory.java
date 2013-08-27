package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public interface CorpusLoaderFactory<T> {
	Iterable<T> buildLoader(Configuration conf, String pathName,
			LineParser<T> lineParser) throws IOException;
}
