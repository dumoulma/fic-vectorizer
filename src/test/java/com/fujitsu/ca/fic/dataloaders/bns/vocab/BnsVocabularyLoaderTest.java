package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.Ignore;
import org.junit.Test;

import com.fujitsu.ca.fic.dataloaders.hdfs.HDFSVocabLoaderFactory;

public class BnsVocabularyLoaderTest {
    private Configuration conf = new Configuration();

    @Ignore
    public void readVocabularyDoesntThrowException() throws IOException {
	String inputDirName = "data/test/sieve/bns-vocab";

	BnsVocabularyLoader bnsVL = new BnsVocabularyLoader(new HDFSVocabLoaderFactory<Double>());
	bnsVL.loadFromText(conf, inputDirName, new BnsVocabLineParser());
    }

    @Test
    public void readVocabularyReturnsVectorOK() throws IOException {
	String inputDirName = "data/test/sieve/bns-vocab";

	BnsVocabularyLoader bnsVL = new BnsVocabularyLoader(new HDFSVocabLoaderFactory<Double>());
	List<String> vocab = bnsVL.loadFromText(conf, inputDirName, new BnsVocabLineParser());
	assertThat(vocab, hasSize(10));
    }

}
