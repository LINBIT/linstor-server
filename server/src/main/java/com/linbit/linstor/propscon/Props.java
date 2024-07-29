package com.linbit.linstor.propscon;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import javax.annotation.Nullable;

/**
 * Common interface for Containers that hold linstor property maps
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface Props extends TransactionObject, ReadOnlyProps
{
    @Nullable
    String setProp(String key, String value)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException;

    @Nullable
    String setProp(String key, String value, @Nullable String namespace)
        throws InvalidKeyException, InvalidValueException, AccessDeniedException, DatabaseException;

    @Nullable
    String removeProp(String key)
        throws InvalidKeyException, AccessDeniedException, DatabaseException;

    @Nullable
    String removeProp(String key, @Nullable String namespace)
        throws InvalidKeyException, AccessDeniedException, DatabaseException;
    boolean removeNamespace(String namespaceRef)
        throws AccessDeniedException, DatabaseException;

    @Override
    @Nullable Props getNamespace(String namespace);

    void loadAll() throws DatabaseException, AccessDeniedException;

    void clear() throws AccessDeniedException, DatabaseException;

    void delete() throws AccessDeniedException, DatabaseException;
}
