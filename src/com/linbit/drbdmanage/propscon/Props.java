package com.linbit.drbdmanage.propscon;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.security.AccessDeniedException;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Common interface for Containers that hold drbdmanage property maps
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Props extends TransactionObject
{
    public String getProp(String key)
        throws InvalidKeyException;
    public String getProp(String key, String namespace)
        throws InvalidKeyException;

    public String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException;
    public String setProp(String key, String value, String namespace)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, SQLException;

    public String removeProp(String key)
        throws InvalidKeyException, AccessDeniedException, SQLException;
    public String removeProp(String key, String namespace)
        throws InvalidKeyException, AccessDeniedException, SQLException;

    public void clear() throws AccessDeniedException, SQLException;

    public int size();
    public boolean isEmpty();

    public String getPath();

    public Map<String, String> map();
    public Set<Map.Entry<String, String>> entrySet();
    public Set<String> keySet();
    public Collection<String> values();

    public Iterator<Map.Entry<String, String>> iterator();
    public Iterator<String> keysIterator();
    public Iterator<String> valuesIterator();

    public Props getNamespace(String namespace)
        throws InvalidKeyException;
    public Iterator<String> iterateNamespaces();
}
