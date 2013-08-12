package com.fujitsu.ca.fic.dataloaders.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.math.Vector;
import org.hamcrest.collection.IsIterableWithSize;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.fujitsu.ca.fic.dataloaders.LineParser;

import static org.hamcrest.CoreMatchers.equalTo;

import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class HdfsCorpusLoaderTest {
    static Configuration realConf = new Configuration();

    @Mock
    LineParser<Vector> lineParser;

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void createIteratorDoesntThrowExceptionForValidPath() throws IOException {
        new HdfsCorpusLoader<>(realConf, "data/bns-corpus", lineParser);
    }

    @Test
    public void iteratorOnOneFileIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/one-file-3lines");

        Iterable<Vector> it = new HdfsCorpusLoader<>(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(equalTo(3)));
    }

    @Test
    public void iteratorOnOneLargeFilesIteratesToTheEnd() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus");

        Iterable<Vector> it = new HdfsCorpusLoader<>(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(599));
    }

    @Test
    public void iteratorOnTwoFilesIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/two-files-6lines");

        Iterable<Vector> it = new HdfsCorpusLoader<>(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(6));
    }

    @Test
    public void iteratorOnThreeFilesIteratesCorrectNumberOfTimes() throws IOException {
        Path inputPath = new Path("data/test/bns-corpus/three-files-9lines");

        Iterable<Vector> it = new HdfsCorpusLoader<>(realConf, inputPath.toString(), lineParser);

        assertThat(it, IsIterableWithSize.<Vector> iterableWithSize(9));
    }
}
