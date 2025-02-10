package com.linbit.utils;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

public class CollectionUtils
{
    /**
     * Checks if a collection is null or empty, both resulting in true.
     * @param collection to be checked
     * @return true if either null or empty.
     */
    public static boolean isEmpty(@Nullable Collection<?> collection)
    {
        return collection == null || collection.isEmpty();
    }

    public static <E, T extends Collection<E>> T lazyCreate(@Nullable T collection, Supplier<T> supplierRef)
    {
        return collection == null ? supplierRef.get() : collection;
    }

    public static <K, V, MAP_IN extends Map<K, V>, MAP_OUT extends Map<K, V>> MAP_OUT createOrWrap(
        MAP_IN mapRef,
        Supplier<MAP_OUT> supplierIfNullRef,
        Function<MAP_IN, MAP_OUT> wrapperIfNonNullREf
    )
    {
        return mapRef == null ? supplierIfNullRef.get() : wrapperIfNonNullREf.apply(mapRef);
    }

    @SuppressWarnings("unchecked")
    public static <E, LIST extends List<E>> LIST nonNullOrEmptyList(LIST listRef)
    {
        return listRef == null ? (LIST) Collections.emptyList() : listRef;
    }
}
