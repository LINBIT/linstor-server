package com.linbit.utils;

import com.linbit.GenericName;
import com.linbit.linstor.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    public static <E, LIST extends List<E>> LIST nonNullOrEmptyList(@Nullable LIST listRef)
    {
        return listRef == null ? (LIST) Collections.emptyList() : listRef;
    }

    public static boolean contains(String strRef, @Nullable Collection<? extends GenericName> collectionRef)
    {
        boolean ret = false;
        String upperStr = strRef.toUpperCase();
        if (collectionRef != null)
        {
            for (GenericName name : collectionRef)
            {
                if (name.value.equals(upperStr))
                {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    public static boolean contains(GenericName nameRef, @Nullable Collection<String> collectionRef)
    {
        boolean ret = false;
        if (collectionRef != null)
        {
            for (String str : collectionRef)
            {
                if (str.equalsIgnoreCase(nameRef.value))
                {
                    ret = true;
                    break;
                }
            }
        }
        return ret;
    }

    /**
     * Converts a given Collection of {@link GenericName}s into a <code>Set&lt;String&gt;</code> containing the
     * upper-case version of the given {@link GenericName}s. Please note that the returned object is a set and not
     * a list, i.e. neither order nor duplicates are preserved.
     *
     * @return Always returns a non-<code>null</code> set even if the input parameter itself is <code>null</code>. In
     *      the latter case the returned set is empty. If a <code>null</code>-check needs to be performed you should
     *      check the original set instead of the returned set.
     */
    public static <T extends GenericName> Set<String> asUpperStringSetFromGenericName(
        @Nullable Collection<T> collectionRef
    )
    {
        Set<String> ret;
        if (collectionRef != null)
        {
            ret = new HashSet<>();
            for (T elem : collectionRef)
            {
                ret.add(elem.value);
            }
        }
        else
        {
            ret = Collections.emptySet();
        }
        return ret;
    }

    /**
     * Converts a given Collection of <code>String</code>s into a <code>Set&lt;String&gt;</code> containing the
     * upper-case version of the given <code>String</code>s. Please note that the returned object is a set and not
     * a list, i.e. neither order nor duplicates are preserved.
     *
     * @return Always returns a non-<code>null</code> set even if the input parameter itself is <code>null</code>. In
     *      the latter case the returned set is empty. If a <code>null</code>-check needs to be performed you should
     *      check the original set instead of the returned set.
     */
    public static Set<String> asUpperStringSet(@Nullable Collection<String> collectionRef)
    {
        Set<String> ret;
        if (collectionRef != null)
        {
            ret = new HashSet<>();
            for (String str : collectionRef)
            {
                ret.add(str.toUpperCase());
            }
        }
        else
        {
            ret = Collections.emptySet();
        }
        return ret;
    }
}
