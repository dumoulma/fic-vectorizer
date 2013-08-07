package com.fujitsu.ca.fic.dataloaders.bnsvocab;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.pig.impl.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BnsVocabularyLoader {
    private static final Logger LOG = LoggerFactory.getLogger(BnsVocabularyLoader.class);

    private static FileSystem fs;
    private static FileStatus[] fileStatus;
    private static int currentFileStatusIndex = 0;
    private static BufferedReader reader = null;
    private static String nextLine = null;
    private static final Map<String, Double> bnsMap = Maps.newHashMap();
    private static final List<String> tokenIndexList = Lists.newArrayList();

    public static List<String> loadFromText(Configuration conf, String pathName) throws IOException {
        fs = FileSystem.get(conf);
        fileStatus = fs.listStatus(new Path(pathName), new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().matches("part(.*)");
            }
        });

        if (fileStatus.length > 0) {
            FileStatus file = fileStatus[currentFileStatusIndex];
            reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
        }

        while (hasNext()) {
            Pair<String, Double> nextPair = getNext();
            bnsMap.put(nextPair.first, nextPair.second);
            tokenIndexList.add(nextPair.first);
        }

        return tokenIndexList;
    }

    /**
     * Will read the next line from input and return true if successful, false if not. This loader will try to open the next file in the
     * directory if at the end of the current one, therefore false is only on the next call after the last line of the last file is read.
     * 
     * @see com.fujitsu.ca.fic.dataloaders.bnsMap.DynamicLoadable#hasNext()
     */
    public static boolean hasNext() {
        if (reader == null)
            return false;
        try {
            nextLine = reader.readLine();
            if (!currentFileHasNextLine(nextLine) && directoryHasMoreFiles()) {
                FileStatus file = fileStatus[++currentFileStatusIndex];
                reader = new BufferedReader(new InputStreamReader(fs.open(file.getPath())));
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

    private static boolean directoryHasMoreFiles() {
        return currentFileStatusIndex + 1 < fileStatus.length;
    }

    private static boolean currentFileHasNextLine(String nextLine2) {
        return (nextLine != null && nextLine.length() > 0);
    }

    /**
     * Returns the pair of value read by hasNext().
     * 
     * @see com.fujitsu.ca.fic.dataloaders.bnsMap.DynamicLoadable#getNext()
     */
    public static Pair<String, Double> getNext() throws IncorrectLineFormatException {
        return parseFields(nextLine);
    }

    private static Pair<String, Double> parseFields(String line) throws IncorrectLineFormatException {
        String token;
        Double bnsScore;

        try {
            String[] fields = line.split(",");
            if (fields.length < 3 || fields.length > 4) {
                String message = "parseFields: unexpected number of fields for line: " + line;
                LOG.warn(message);
                throw new IncorrectLineFormatException(message);
            }

            if (fields.length == 4) {
                fields[0] = fields[0] + "," + fields[1];
                fields[1] = fields[2];
            }
            token = fields[0];
            bnsScore = Double.parseDouble(fields[1]);

        } catch (Exception e) {
            LOG.warn("parseFields: could not parse line: " + line);
            throw new IncorrectLineFormatException("Could not parse line: \n" + line);
        }
        LOG.debug(String.format("Pair: <%s, %f>", token, bnsScore));
        return new Pair<String, Double>(token, bnsScore);
    }
}
