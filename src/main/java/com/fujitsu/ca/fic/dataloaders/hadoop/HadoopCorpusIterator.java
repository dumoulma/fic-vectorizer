package com.fujitsu.ca.fic.dataloaders.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.mahout.math.Vector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.dataloaders.CorpusIterator;
import com.fujitsu.ca.fic.dataloaders.LineParser;
import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public class HadoopCorpusIterator extends CorpusIterator<Vector> {
    private static Logger LOG = LoggerFactory.getLogger(HadoopCorpusIterator.class);

    LineParser lineParser;
    private static FileSystem fs;
    private static FileStatus[] fileStatus;
    private static int currentFileStatusIndex = 0;
    private static BufferedReader reader = null;
    private static String nextLine = null;

    public HadoopCorpusIterator(Configuration conf, String inputDirName, LineParser lineParser) throws IOException {
        this.lineParser = lineParser;
        FileSystem fs = FileSystem.get(conf);
        fileStatus = fs.listStatus(new Path(inputDirName), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part(.*)");
            }
        });
    }

    @Override
    public boolean hasNext() {
        if (reader == null)
            return false;
        try {
            nextLine = reader.readLine();
            if (!currentFileHasNextLine(nextLine) && directoryHasMoreFiles()) {
                setReaderToNextFile(fs);

                hasNext();
            } else if (currentFileHasNextLine(nextLine)) {
                return true;
            }
            reader.close();

        } catch (IOException e) {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return false;
    }

    private boolean directoryHasMoreFiles() {
        return currentFileStatusIndex < fileStatus.length;
    }

    private boolean currentFileHasNextLine(String nextLine2) {
        return (nextLine != null && nextLine.length() > 0);
    }

    private void setReaderToNextFile(FileSystem fs) throws IOException {
        FileStatus file = fileStatus[currentFileStatusIndex++];
        Path nextFilePath = file.getPath();
        LOG.info("Processing next file: " + nextFilePath.getName());
        reader = new BufferedReader(new InputStreamReader(fs.open(nextFilePath)));
    }

    @Override
    public Vector next() {
        Vector nextDoc = null;
        try {
            nextDoc = lineParser.parseFields(nextLine);
        } catch (IncorrectLineFormatException e) {
            LOG.warn(e.toString());
        }
        return nextDoc;
    }

    @Override
    public Iterator<Vector> iterator() {
        return this;
    }

}
