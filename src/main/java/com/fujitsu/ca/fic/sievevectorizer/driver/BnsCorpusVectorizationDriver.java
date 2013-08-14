package com.fujitsu.ca.fic.sievevectorizer.driver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.VocabularyLoader;
import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabularyLoader;

/**
 * The CorpusVectorizationDriver is the main program to launch the MR jobs that will take a corpus of unstructured text split into two
 * classes, each in its own folder, and using an existing dictionary of BNS term
 */
public class BnsCorpusVectorizationDriver extends Configured implements Tool {
    private static Logger LOG = LoggerFactory.getLogger(BnsCorpusVectorizationDriver.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BnsCorpusVectorizationDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        Configuration conf = getConf();

        String vocabDir = conf.get("data.vocab.path");
        String trainDir = conf.get("data.corpus.train.path");
        String testDir = conf.get("data.corpus.test.path");
        String outputFilename = conf.get("data.sequence.output.path");

        if (vocabDir == null | trainDir == null | testDir == null | outputFilename == null) {
            LOG.error("The configuration file was not loaded correctly! Please check: \n" + "data.vocab.path \n"
                    + "data.corpus.train.path \n" + "data.corpus.test.path \n" + "data.sequence.output.path \n");
            throw new IllegalStateException("The expected configuration values for data paths have not been found.");
        }

        LOG.info("Loading vocabulary from path: " + vocabDir);
        VocabularyLoader vocabLoader = new BnsVocabularyLoader();
        List<String> tokenIndexList = vocabLoader.loadFromText(conf, vocabDir);
        int vocabSize = tokenIndexList.size();
        LOG.info("The vocab file has been loaded successfully with " + vocabSize + " entries.");

        CorpusVectorizer corpus = new BnsCorpusVectorizer();
        LOG.info("Vectorizing train documents...");
        corpus.convertToSequenceFile(conf, vocabSize, trainDir, outputFilename + "/train.seq");
        LOG.info("Vectorizing test documents...");
        corpus.convertToSequenceFile(conf, vocabSize, testDir, outputFilename + "/test.seq");
        LOG.info("BNS Vectorization successful!");
        return Job.SUCCESS;
    }
}
