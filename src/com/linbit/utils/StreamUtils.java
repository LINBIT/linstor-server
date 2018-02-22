package com.linbit.utils;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils
{
    public static <T> Stream<T> toStream(Iterator<T> iterator)
    {
        return toStream(iterator, false);
    }

    public static <T> Stream<T> toStream(Iterator<T> iterator, boolean parallel)
    {
        return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(
                iterator, Spliterator.ORDERED),
            parallel
        );
    }
}
