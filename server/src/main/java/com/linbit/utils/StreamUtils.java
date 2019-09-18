package com.linbit.utils;

import com.linbit.linstor.security.AccessDeniedException;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class StreamUtils
{
    @FunctionalInterface
    public interface AccessCheckedConsumer<T>
    {
        void accept(T element) throws AccessDeniedException;
    }

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

    public static <E> Consumer<E> accChecked(
        AccessCheckedConsumer<E> consumer,
        Consumer<AccessDeniedException> excHandler
    )
    {
        return elem ->
        {
            try
            {
                consumer.accept(elem);
            }
            catch (AccessDeniedException accDeniedExc)
            {
                excHandler.accept(accDeniedExc);
            }
        };
    }

    private StreamUtils()
    {
    }
}
