package com.fujitsu.ca.fic.dataloaders;

import java.util.Iterator;

public abstract class CorpusIterator<E> implements Iterator<E>, Iterable<E> {
    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
