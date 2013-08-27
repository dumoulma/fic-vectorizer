package com.fujitsu.ca.fic.dataloaders.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;

import com.fujitsu.ca.fic.dataloaders.CorpusLoaderFactory;
import com.fujitsu.ca.fic.dataloaders.LineParser;

public class HDFSCorpusLoaderFactory implements CorpusLoaderFactory<Vector> {
	@Override
	public Iterable<Vector> buildLoader(Configuration conf, String pathName,
			LineParser<Vector> lineParser) throws IOException {
		return new HdfsCorpusLoader<Vector>(conf, pathName, lineParser);
	}

}