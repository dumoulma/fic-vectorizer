package com.fujitsu.ca.fic.dataloaders.bns;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.mahout.math.Vector;
import org.apache.pig.impl.util.Pair;

import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusLineParser;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabLineParser;
import com.fujitsu.ca.fic.dataloaders.hdfs.HdfsCorpusLoader;

public class BnsCorpusFactory {
    public static HdfsCorpusLoader<Vector> createHdfsCorpusLoader(Configuration conf, String inputDirName, List<String> tokenIndexList)
            throws IOException {
        return new HdfsCorpusLoader<Vector>(conf, inputDirName, new BnsCorpusLineParser(tokenIndexList));
    }

    public static HdfsCorpusLoader<Pair<String, Double>> createHdfsVocabLoader(Configuration conf, String pathName) throws IOException {
        return new HdfsCorpusLoader<Pair<String, Double>>(conf, pathName, new BnsVocabLineParser());
    }
}
