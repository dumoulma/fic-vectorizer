package com.fujitsu.ca.fic.dataloaders.bns;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;
import org.apache.pig.impl.util.Pair;

import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusLineParser;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabLineParser;
import com.fujitsu.ca.fic.dataloaders.hdfs.HdfsCorpusLoader;

public abstract class BnsCorpusFactory {
    private BnsCorpusFactory() {
    }

    public static HdfsCorpusLoader<Vector> createHdfsCorpusLoader(
            Configuration conf, String inputDirName) throws IOException {
        return new HdfsCorpusLoader<Vector>(conf, inputDirName,
                new BnsCorpusLineParser());
    }

    public static HdfsCorpusLoader<Pair<String, Double>> createHdfsVocabLoader(
            Configuration conf, String pathName) throws IOException {
        return new HdfsCorpusLoader<Pair<String, Double>>(conf, pathName,
                new BnsVocabLineParser());
    }

}
