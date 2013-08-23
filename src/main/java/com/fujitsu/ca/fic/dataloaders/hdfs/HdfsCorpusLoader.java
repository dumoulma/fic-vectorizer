package com.fujitsu.ca.fic.dataloaders.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.google.common.collect.Lists;

public class HdfsCorpusLoader<E> implements Iterable<E> {
    private static Logger log = LoggerFactory.getLogger(HdfsCorpusLoader.class);
    private final HadoopCorpusIterator corpusItr;

    public HdfsCorpusLoader(Configuration conf, String inputDirName,
            LineParser<E> lineParser) throws IOException {
        corpusItr = new HadoopCorpusIterator(conf, inputDirName, lineParser);
    }

    @Override
    public Iterator<E> iterator() {
        return corpusItr;
    }

    public class HadoopCorpusIterator implements Iterator<E> {
        private final LineParser<E> lineParser;
        private final FileSystem fs;
        private final List<Path> filesToProcess;
        private int currentFileStatusIndex = 0;
        private BufferedReader reader = null;
        private String nextLine = null;

        public HadoopCorpusIterator(Configuration conf, String inputDirName,
                LineParser<E> lineParser) throws IOException {
            this.lineParser = lineParser;
            fs = FileSystem.get(conf);
            filesToProcess = getListOfMapReduceOutputFiles(fs, inputDirName);
        }

        private List<Path> getListOfMapReduceOutputFiles(FileSystem fs1,
                String inputDirName) throws IOException {
            FileStatus[] fileStatus = fs1.listStatus(new Path(inputDirName),
                    new PathFilter() {
                        @Override
                        public boolean accept(Path path) {
                            return path.getName().matches("part(.*)");
                        }
                    });
            List<Path> paths = Lists.newArrayList();
            for (FileStatus file : fileStatus) {
                paths.add(file.getPath());
            }
            return paths;
        }

        @Override
        public boolean hasNext() {
            try {
                if (reader == null
                        && currentFileStatusIndex == filesToProcess.size()) {
                    return false;
                } else if (reader == null) {
                    setReaderToNextFile();
                }
                nextLine = reader.readLine();
                boolean isEOF = isEndOfFile(nextLine);
                if (isEOF && directoryHasMoreFiles()) {
                    log.debug("File finished, changing to next file.");
                    setReaderToNextFile();
                    return hasNext();

                } else if (!isEOF) {
                    return true;
                }
                reader.close();
                reader = null;
            } catch (IOException e) {
                log.error(e.toString());
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e2) {
                        log.error(e.toString());
                    }
                }
            }
            return false;
        }

        private boolean directoryHasMoreFiles() {
            return currentFileStatusIndex < filesToProcess.size();
        }

        private boolean isEndOfFile(String nextLine1) {
            return (nextLine1 == null || nextLine1.isEmpty());
        }

        private void setReaderToNextFile() throws IOException {
            if (currentFileStatusIndex == filesToProcess.size()) {
                log.warn("setReaderToNextFile: No more files to process!");
                reader = null;
                return;
            }
            Path nextFilePath = filesToProcess.get(currentFileStatusIndex++);
            reader = new BufferedReader(new InputStreamReader(
                    fs.open(nextFilePath)));

            log.info("Processing next file: " + nextFilePath.toString());
        }

        @Override
        public E next() {
            E nextDoc = null;
            nextDoc = lineParser.parseFields(nextLine);
            return nextDoc;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}
