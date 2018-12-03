package com.linbit.utils;

import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Transforms a flux by ordering the emissions based on a sequence number.
 * The sequence number must be zero-based.
 */
public class OrderingFlux<T>
{
    private final Flux<T> out;

    private final NavigableMap<Long, T> buffer = new TreeMap<>();
    private long nextIndex = 0;

    public static <T> Flux<T> order(Flux<Tuple2<Long, T>> flux)
    {
        return new OrderingFlux<>(flux).get();
    }

    public OrderingFlux(Flux<Tuple2<Long, T>> flux)
    {
        out = flux
            // buffer values if necessary to ensure that the output is ordered,
            // emitting groups of values when they are ready and flattening the value groups
            .flatMapIterable(this::handleNext);
    }

    public Flux<T> get()
    {
        return out;
    }

    private List<T> handleNext(Tuple2<Long, T> indexedValue)
    {
        buffer.put(indexedValue.getT1(), indexedValue.getT2());
        List<T> values = new ArrayList<>();
        while (!buffer.isEmpty() && buffer.firstKey() == nextIndex)
        {
            T nextValue = buffer.remove(buffer.firstKey());
            values.add(nextValue);
            nextIndex++;
        }
        return values;
    }
}
