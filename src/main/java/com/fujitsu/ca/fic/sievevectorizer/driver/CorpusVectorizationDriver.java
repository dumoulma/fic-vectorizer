package com.fujitsu.ca.fic.sievevectorizer.driver;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bnscorpus.BnsCorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bnsvocab.BnsVocabularyLoader;

/**
 * The CorpusVectorizationDriver is the main program to launch the MR jobs that will take a corpus of unstructured text split into two
 * classes, each in its own folder, and using an existing dictionary of BNS term
 */
public class CorpusVectorizationDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new CorpusVectorizationDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        List<String> tokenIndexList = BnsVocabularyLoader.loadFromText(conf, "data/out/bns-vocab");
        System.out.println(String.format("VocabSize=%d", tokenIndexList.size()));

        CorpusVectorizer corpus = new BnsCorpusVectorizer();
        corpus.convertToSequenceFile(conf, tokenIndexList, "data/out/bns-corpus", "data/out/corpus-sequences");

        return 0;
    }
}
