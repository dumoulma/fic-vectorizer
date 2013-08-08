package com.fujitsu.ca.fic.dataloaders;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

/**
 * This is the only interface that needs to be implemented to suit the format of a given line of text of a document in HDFS. The output is
 * generic and might be a Mahout Vector or a Tuple or terms or anything else. Documents should be read through an iterable so memory is kept
 * at a minimum.
 * 
 * @author dumoulma
 * 
 * @param <T>
 *            The output of a parsed line of raw text
 */
public interface LineParser<T> {
    T parseFields(String line) throws IncorrectLineFormatException;
}
