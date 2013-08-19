package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

/**
 * A corpus vectorizer can convert the output of a Pig job in a given input folder in HDFS (or locally) and will produce a sequence file of
 * Mahout NamedVectors ready for use by Mahout algorithms.
 * <p>
 * Note: cardinality is the size of the vectors to be converted. It should be equal to the size of the vocabulary of the corpus.
 * </p>
 * 
 * @author dumoulma
 * 
 */
public interface CorpusVectorizer {
    void convertToSequenceFile(Configuration conf, String inputDirName, String outputDirName) throws IOException;
}
