package com.fujitsu.ca.fic.dataloaders;

import com.fujitsu.ca.fic.exceptions.IncorrectLineFormatException;

public interface LineParser<T> {
    T parseFields(String line) throws IncorrectLineFormatException;
}
