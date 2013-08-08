package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

/**
 * A corpus vectorizer can convert the output of a Pig job in a given input folder in HDFS (or locally) and will produce a sequence file of
 * Mahout NamedVectors ready for use by Mahout algorithms.
 * 
 * @author dumoulma
 * 
 */
public interface CorpusVectorizer {
    void convertToSequenceFile(Configuration conf, List<String> tokenIndexList, String inputDirName, String outputDirName)
            throws IOException;
}
