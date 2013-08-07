package com.fujitsu.ca.fic.dataloaders.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.hamcrest.collection.IsIterableWithSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fujitsu.ca.fic.dataloaders.CorpusIterator;
import com.fujitsu.ca.fic.dataloaders.LineParser;

import static org.hamcrest.MatcherAssert.assertThat;

//import static org.mockito.Matchers.anyObject;
//import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class HadoopCorpusIteratorTest {
    static Configuration realConf = new Configuration();

    @Mock
    Configuration mockConf;

    @Mock
    FileSystem mockFs;

    @Mock
    LineParser lineParser;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Ignore
    public void createIteratorDoesntThrowExceptionForValidPath() throws IOException {
        new HadoopCorpusIterator(realConf, "data/bns-corpus", lineParser);
    }

    @Ignore
    public void iteratorOnOneFileIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/one-file-3lines");

        CorpusIterator<Vector> it = new HadoopCorpusIterator(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(3));
    }

    @Ignore
    public void iteratorOnOneLargeFilesIteratesToTheEnd() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus");

        CorpusIterator<Vector> it = new HadoopCorpusIterator(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(599));
    }

    @Test
    public void iteratorOnTwoFilesIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/two-files-6lines");

        CorpusIterator<Vector> it = new HadoopCorpusIterator(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(6));
    }

    @Test
    public void iteratorOnThreeFilesIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/three-files-9lines");

        CorpusIterator<Vector> it = new HadoopCorpusIterator(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(9));
    }
}
