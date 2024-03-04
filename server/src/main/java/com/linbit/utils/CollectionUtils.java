package com.linbit.utils;

import javax.annotation.Nullable;

import java.util.Collection;

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
}
