package com.fujitsu.ca.fic.sievevectorizer.driver;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.HadoopUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusLineParser;
import com.fujitsu.ca.fic.dataloaders.bns.corpus.BnsCorpusVectorizer;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabLineParser;
import com.fujitsu.ca.fic.dataloaders.bns.vocab.BnsVocabularyLoader;
import com.fujitsu.ca.fic.dataloaders.hdfs.HDFSCorpusLoaderFactory;
import com.fujitsu.ca.fic.dataloaders.hdfs.HDFSVocabLoaderFactory;

/**
 * This is the driver that launches the MapReduce jobs.
 * <p>
 * The driver will take as input a corpus of unstructured text split into two
 * classes, each in its own folder. The input documents are the output from
 * bns.pig.
 * </p>
 * The output will be a test and train Hadoop SequenceFile:
 * <DocumentName:Text,NamedVector:VectorWritable>
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

	String vocabDir = "data/out/sieve/bns/spam-vs-rel/bns-vocab"; // conf.get("data.vocab.path");
	String trainDir = "data/out/sieve/bns/spam-vs-rel/train"; // conf.get("data.corpus.train.path");
	String testDir = "data/out/sieve/bns/spam-vs-rel/test"; // conf.get("data.corpus.test.path");
	String outputDirName = "data/out/sieve/bns/rel-vs-notrel"; // conf.get("data.sequence.output.path");

	if (vocabDir == null | trainDir == null | testDir == null | outputDirName == null) {
	    log.error("The configuration file was not loaded correctly! Please check: \n"
		    + "data.vocab.path \n" + "data.corpus.train.path \n"
		    + "data.corpus.test.path \n" + "data.sequence.output.path \n");
	    throw new IllegalStateException(
		    "The expected configuration values for data paths have not been found.");
	}

	log.info("Loading vocabulary from path: " + vocabDir);
	List<String> tokenIndexList = new BnsVocabularyLoader(new HDFSVocabLoaderFactory<Double>())
		.loadFromText(conf, vocabDir, new BnsVocabLineParser());
	int vocabCardinality = tokenIndexList.size();
	log.info("The vocab file has been loaded successfully with " + vocabCardinality
		+ " entries.");

	Path outputPath = new Path(outputDirName);
	HadoopUtil.delete(conf, outputPath);

	CorpusVectorizer corpus = new BnsCorpusVectorizer(new HDFSCorpusLoaderFactory());
	log.info("Vectorizing train documents...");
	corpus.convertToSequenceFile(conf, trainDir, outputDirName + "/train.seq",
		new BnsCorpusLineParser());
	log.info("Vectorizing test documents...");
	corpus.convertToSequenceFile(conf, testDir, outputDirName + "/test.seq",
		new BnsCorpusLineParser());
	log.info("BNS Vectorization successful!");
	return Job.SUCCESS;
    }
}
