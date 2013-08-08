package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

public interface VocabularyLoader {

    List<String> loadFromText(Configuration conf, String pathName) throws IOException;

}
