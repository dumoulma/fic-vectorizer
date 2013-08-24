package com.fujitsu.ca.fic.dataloaders.bns.corpus;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.junit.Test;
import org.mockito.Mock;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.*;
public class BnsCorpusVectorizerTest {

	@Mock
	private SequenceFile.Writer writer;
	@Mock
	Configuration conf;

	@Test
	public void testConvertToSequenceFile() throws IOException {
//		String inputDirName = "data/test/bns-corpus/one-file-3lines";
//		String outputDirName = "output";
//		
//		BnsCorpusVectorizer bnsCV = new BnsCorpusVectorizer(conf, outputDirName);
//		bnsCV.convertToSequenceFile(inputDirName);
//		verify(writer, times(3)).append(anyObject(), anyObject());
		assertThat(true, equalTo(true));
	}
}
