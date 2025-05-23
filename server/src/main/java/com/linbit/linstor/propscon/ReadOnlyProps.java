package com.linbit.linstor.propscon;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.api.prop.LinStorObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ReadOnlyProps extends Iterable<Map.Entry<String, String>>
{
    String PATH_SEPARATOR = "/";

    String getDescription();

    LinStorObject getType();

    @Nullable
    String getProp(String key)
        throws InvalidKeyException;

    String getPropWithDefault(String key, String defaultValue) throws InvalidKeyException;

    @Nullable
    String getProp(String key, @Nullable String namespace)
        throws InvalidKeyException;

    String getPropWithDefault(String key, @Nullable String namespace, String defaultValue)
        throws InvalidKeyException;

    int size();

    boolean isEmpty();

    String getPath();

    Map<String, String> map();

    Map<String, String> cloneMap();

    Set<Map.Entry<String, String>> entrySet();

    Set<String> keySet();

    Collection<String> values();

    @Override
    Iterator<Map.Entry<String, String>> iterator();

    Iterator<String> keysIterator();

    Iterator<String> valuesIterator();

    @Nullable
    ReadOnlyProps getNamespace(@Nullable String namespace);

    default ReadOnlyProps getNamespaceOrEmpty(String namespace)
    {
        @Nullable ReadOnlyProps roProps = getNamespace(namespace);
        if (roProps == null)
        {
            roProps = ReadOnlyPropsImpl.emptyRoProps();
        }
        return roProps;
    }

    Iterator<String> iterateNamespaces();


    /**
     * Checks if all propFilters (key value pairs e.g 'prop=value') are present in the given Props container.
     * It is also possible to just check if a property is set at all.
     *
     * @param propFilters List of filter pairs, e.g.: ['site=a', 'zfsnode']
     * @return True if all props match.
     */
    default boolean contains(List<String> propFilters)
    {
        boolean result = true;
        if (!propFilters.isEmpty())
        {
            for (final String pFilter : propFilters)
            {
                String[] split = pFilter.split("=", 2);
                @Nullable String value = getProp(split[0]);
                if (value == null)
                {
                    result = false;
                    break;
                }
                else if (split.length > 1)
                {
                    if (!value.equals(split[1]))
                    {
                        result = false;
                        break;
                    }
                }
            }
        }
        return result;
    }

    /**
     * Checks if a boolean property is considered true.
     *
     * @param key property key
     * @return true if the key value is "true" in any casing.
     * @throws InvalidKeyException
     */
    default boolean isPropTrue(String key) throws InvalidKeyException
    {
        return getPropWithDefault(key, "false").equalsIgnoreCase("true");
    }
}
