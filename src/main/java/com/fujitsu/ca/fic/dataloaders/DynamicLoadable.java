package com.fujitsu.ca.fic.dataloaders;

import org.apache.pig.impl.util.Pair;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public interface DynamicLoadable {

    public abstract boolean hasNext();

    public abstract Pair<String, Double> getNext() throws IncorrectLineFormatException;

}
