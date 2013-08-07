package com.fujitsu.ca.fic.dataloaders;

import org.apache.mahout.math.Vector;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public interface LineParser {
    Vector parseFields(String line) throws IncorrectLineFormatException;
}
