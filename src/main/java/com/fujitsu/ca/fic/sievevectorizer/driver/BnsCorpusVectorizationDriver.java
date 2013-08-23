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
import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabularyLoader;

/**
 * The CorpusVectorizationDriver is the main program to launch the MR jobs that will take a corpus of unstructured text split into two
 * classes, each in its own folder, and using an existing dictionary of BNS term
 */
public class BnsCorpusVectorizationDriver extends Configured implements Tool {
    private static Logger log = LoggerFactory.getLogger(BnsCorpusVectorizationDriver.class);

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new BnsCorpusVectorizationDriver(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws IOException {
        Configuration conf = getConf();

        String vocabDir = "data/out/bns-corpus/spam-vs-rel/bns-vocab"; // conf.get("data.vocab.path");
        String trainDir = "data/out/bns-corpus/spam-vs-rel/train"; // conf.get("data.corpus.train.path");
        String testDir = "data/out/bns-corpus/spam-vs-rel/test"; // conf.get("data.corpus.test.path");
        String outputFilename = "data/out/bns-corpus/spam-vs-rel/"; // conf.get("data.sequence.output.path");

        if (vocabDir == null | trainDir == null | testDir == null | outputFilename == null) {
            log.error("The configuration file was not loaded correctly! Please check: \n" + "data.vocab.path \n"
                    + "data.corpus.train.path \n" + "data.corpus.test.path \n" + "data.sequence.output.path \n");
            throw new IllegalStateException("The expected configuration values for data paths have not been found.");
        }

        log.info("Loading vocabulary from path: " + vocabDir);
        List<String> tokenIndexList = new BnsVocabularyLoader().loadFromText(conf, vocabDir);
        int vocabCardinality = tokenIndexList.size();
        log.info("The vocab file has been loaded successfully with " + vocabCardinality + " entries.");

        CorpusVectorizer corpus = new BnsCorpusVectorizer();
        log.info("Vectorizing train documents...");
        corpus.convertToSequenceFile(conf, trainDir, outputFilename + "/train.seq");
        log.info("Vectorizing test documents...");
        corpus.convertToSequenceFile(conf, testDir, outputFilename + "/test.seq");
        log.info("BNS Vectorization successful!");
        return Job.SUCCESS;
    }
}
