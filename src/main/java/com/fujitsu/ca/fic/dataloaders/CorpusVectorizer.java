package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public interface CorpusVectorizer {
    void convertToSequenceFile(Configuration conf, List<String> tokenIndexList, String string, String outputDirName) throws IOException;
}
