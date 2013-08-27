package com.fujitsu.ca.fic.dataloaders.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.Pair;

import com.fujitsu.ca.fic.dataloaders.CorpusLoaderFactory;
import com.fujitsu.ca.fic.dataloaders.LineParser;

public class HDFSVocabLoaderFactory<T> implements
		CorpusLoaderFactory<Pair<String, T>> {
	@Override
	public Iterable<Pair<String, T>> buildLoader(Configuration conf,
			String pathName, LineParser<Pair<String, T>> lineParser)
			throws IOException {
		return new HdfsCorpusLoader<Pair<String, T>>(conf, pathName, lineParser);
	}

}
