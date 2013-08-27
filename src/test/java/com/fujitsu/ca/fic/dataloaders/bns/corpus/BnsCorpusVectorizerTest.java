package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.VectorWritable;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.CoreMatchers.*;

import com.fujitsu.ca.fic.dataloaders.hdfs.HDFSCorpusLoaderFactory;

public class BnsCorpusVectorizerTest {
    private Configuration conf = new Configuration();
    private static final String OUTPUT_DIR = "data/test/out/output.seq";
    private Path outputPath = new Path(OUTPUT_DIR);

    @Before
    public void before() throws IOException {
	HadoopUtil.delete(conf, outputPath);
    }
    
    @After
    public void after() throws IOException {
	HadoopUtil.delete(conf, outputPath);
    }

    @Ignore
    public void testConvertToSequenceFileDoesntThrowException() throws IOException {
	String inputDirName = "data/test/sieve/spam-vs-rel";

	BnsCorpusVectorizer bnsCV = new BnsCorpusVectorizer(new HDFSCorpusLoaderFactory());
	bnsCV.convertToSequenceFile(conf, inputDirName, OUTPUT_DIR, new BnsCorpusLineParser());
    }

    @Test
    public void testConvertToSequenceFileCanReadFileBackOK() throws IOException {
	String inputDirName = "data/test/sieve/spam-vs-rel";	
	BnsCorpusVectorizer bnsCV = new BnsCorpusVectorizer(new HDFSCorpusLoaderFactory());
	bnsCV.convertToSequenceFile(conf, inputDirName, OUTPUT_DIR, new BnsCorpusLineParser());

	SequenceFile.Reader reader = new SequenceFile.Reader(FileSystem.get(conf), outputPath,
		conf);

	int recordsReadCount = 0;
	Text key = new Text();
	VectorWritable val = new VectorWritable();
	while (reader.next(key, val)) {
	    recordsReadCount++;
	}
	IOUtils.closeStream(reader);
	assertThat(recordsReadCount, equalTo(25));
    }

}
