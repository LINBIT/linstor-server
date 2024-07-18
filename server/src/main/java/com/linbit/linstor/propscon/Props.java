package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

/**
 * Common interface for Containers that hold linstor property maps
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Props extends TransactionObject, ReadOnlyProps
{
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
}
