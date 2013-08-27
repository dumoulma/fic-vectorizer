package com.fujitsu.ca.fic.dataloaders;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.impl.util.Pair;

/**
 * Loads the vocabulary that allows to transform a text to a Vector Space
 * representation.
 * <p>
 * One such use is to load a given vocabulary and then hand it to a parser that
 * can use it to associate tokens with an index and output Vectors when given a
 * raw unstructured text document.
 * </p>
 * 
 * @author dumoulma
 * 
 */
public interface VocabularyLoader<T> {	
	List<String> loadFromText(Configuration conf, String pathName,
			LineParser<Pair<String, T>> lineParser) throws IOException;
}
