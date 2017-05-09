package com.linbit.drbdmanage.propscon;

import com.linbit.drbdmanage.security.AccessDeniedException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Read-only access to a properties container
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public class ReadOnlyProps implements Props
{
    private Props propsMap;

    public ReadOnlyProps(Props propsRef)
    {
        propsMap = propsRef;
    }

    @Override
    public String getProp(String key) throws InvalidKeyException
    {
        return propsMap.getProp(key);
    }

    @Override
    public String getProp(String key, String namespace) throws InvalidKeyException
    {
        return propsMap.getProp(key, namespace);
    }

    @Override
    public String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // throws UnsupportedOperationException
        denyAccess();

        // Never reached
        return null;
    }

    @Override
    public String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException
    {
        // throws UnsupportedOperationException
        denyAccess();

        // Never reached
        return null;
    }

    @Override
    public String removeProp(String key) throws InvalidKeyException, AccessDeniedException
    {
        // throws UnsupportedOperationException
        denyAccess();

        // Never reached
        return null;
    }

    @Override
    public String removeProp(String key, String namespace)
        throws InvalidKeyException, AccessDeniedException
    {
        // throws UnsupportedOperationException
        denyAccess();

        // Never reached
        return null;
    }

    @Override
    public void clear() throws AccessDeniedException
    {
        // throws UnsupportedOperationException
        denyAccess();
    }

    @Override
    public int size()
    {
        return propsMap.size();
    }

    @Override
    public boolean isEmpty()
    {
        return propsMap.isEmpty();
    }

    @Override
    public String getPath()
    {
        return propsMap.getPath();
    }

    @Override
    public Map<String, String> map()
    {
        return Collections.unmodifiableMap(propsMap.map());
    }

    @Override
    public Set<Map.Entry<String, String>> entrySet()
    {
        return Collections.unmodifiableSet(propsMap.entrySet());
    }

    @Override
    public Set<String> keySet()
    {
        return Collections.unmodifiableSet(propsMap.keySet());
    }

    @Override
    public Collection<String> values()
    {
        return Collections.unmodifiableCollection(propsMap.values());
    }

    @Override
    public Iterator<Map.Entry<String, String>> iterator()
    {
        return new ReadOnlyIterator<>(propsMap.iterator());
    }

    @Override
    public Iterator<String> keysIterator()
    {
        return new ReadOnlyIterator<>(propsMap.keysIterator());
    }

    @Override
    public Iterator<String> valuesIterator()
    {
        return new ReadOnlyIterator<>(propsMap.valuesIterator());
    }

    @Override
    public Props getNamespace(String namespace) throws InvalidKeyException
    {
        return propsMap.getNamespace(namespace);
    }

    private void denyAccess()
        throws AccessDeniedException
    {
        throw new AccessDeniedException(
            "Attempt to modify a read-only view of a properties container"
        );
    }

    private void unsupported()
    {
        throw new UnsupportedOperationException(
            "Attempt to perform an unsupported operation on a read-only view of a properties container"
        );
    }

    static class ReadOnlyIterator<T> implements Iterator<T>
    {
        private Iterator<T> iter;

        ReadOnlyIterator(Iterator<T> iterRef)
        {
            iter = iterRef;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public T next()
        {
            return iter.next();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException(
                "Attempt to modify an object through read-only iterator"
            );
        }
    }
}
