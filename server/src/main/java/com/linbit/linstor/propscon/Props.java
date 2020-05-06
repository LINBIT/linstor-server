package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Common interface for Containers that hold linstor property maps
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Props extends TransactionObject, Iterable<Map.Entry<String, String>>
{
    String PATH_SEPARATOR = "/";

    String getProp(String key)
        throws InvalidKeyException;
    String getPropWithDefault(String key, String defaultValue) throws InvalidKeyException;
    String getProp(String key, String namespace)
        throws InvalidKeyException;
    String getPropWithDefault(String key, String namespace, String defaultValue) throws InvalidKeyException;

    String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException;
    String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException;

    String removeProp(String key)
        throws InvalidKeyException, AccessDeniedException, DatabaseException;
    String removeProp(String key, String namespace)
        throws InvalidKeyException, AccessDeniedException, DatabaseException;
    boolean removeNamespace(String namespaceRef)
        throws AccessDeniedException, DatabaseException;

    void loadAll() throws DatabaseException, AccessDeniedException;

    void clear() throws AccessDeniedException, DatabaseException;

    void delete() throws AccessDeniedException, DatabaseException;

    int size();
    boolean isEmpty();

    String getPath();

    Map<String, String> map();
    Set<Map.Entry<String, String>> entrySet();
    Set<String> keySet();
    Collection<String> values();

    @Override
    Iterator<Map.Entry<String, String>> iterator();
    Iterator<String> keysIterator();
    Iterator<String> valuesIterator();

    Optional<Props> getNamespace(String namespace);
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
        if (!propFilters.isEmpty())
        {
            for (final String pFilter : propFilters)
            {
                String[] split = pFilter.split("=", 2);
                String value = this.getProp(split[0]);
                if (value == null)
                {
                    return false;
                }
                if (split.length > 1)
                {
                    if (!value.equals(split[1]))
                    {
                        return false;
                    }
                }
            }
        }
        return true;
    }
}
