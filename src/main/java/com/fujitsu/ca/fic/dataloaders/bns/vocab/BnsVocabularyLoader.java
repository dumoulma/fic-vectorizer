package com.fujitsu.ca.fic.dataloaders.bns.vocab;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.VocabularyLoader;
import com.fujitsu.ca.fic.dataloaders.bns.BnsCorpusFactory;
import com.fujitsu.ca.fic.dataloaders.hdfs.HdfsCorpusLoader;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BnsVocabularyLoader implements VocabularyLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BnsVocabularyLoader.class);

    private static final Map<String, Double> bnsMap = Maps.newHashMap();
    private static final List<String> tokenIndexList = Lists.newArrayList();

    /**
     * Reads from HDFS a list of tokens with their associated BNS term weights. Returns the list of tokens. We also keep a dictionary with
     * the tokens and their BNS weights. The index in this list is the position of the word in the vector space representation.
     * 
     * <p>
     * NOTE: This list will not likely be larger than 5k to 25k entries, it is safe to keep in memory. The current default targetted
     * vocabulary size is around 5000 words.
     * </p>
     * 
     * 
     * @see com.fujitsu.ca.fic.dataloaders.bnsvocab.VocabularyLoader#loadFromText(org.apache.hadoop.conf.Configuration, java.lang.String)
     */
    @Override
    public List<String> loadFromText(Configuration conf, String pathName) throws IOException {
        HdfsCorpusLoader<Pair<String, Double>> corpusLoader = BnsCorpusFactory.createHdfsVocabLoader(conf, pathName);
        for (Pair<String, Double> nextPair : corpusLoader) {
            LOG.debug("Next Pair: " + nextPair);
            bnsMap.put(nextPair.first, nextPair.second);
            tokenIndexList.add(nextPair.first);
        }
        return tokenIndexList;
    }
}
