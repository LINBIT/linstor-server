package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ProtectedObject;
import com.linbit.linstor.transaction.TransactionObject;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

/**
 * Definition of a storage pool
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPoolDefinition
    extends TransactionObject, DbgInstanceUuid, Comparable<StorPoolDefinition>, ProtectedObject
{
    UUID getUuid();

    StorPoolName getName();

    StorPool getStorPool(AccessContext accCtx, NodeName nodeName)
        throws AccessDeniedException;

    Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    Stream<StorPool> streamStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    StorPoolDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    @Override
    default int compareTo(StorPoolDefinition otherStorPool)
    {
        return getName().compareTo(otherStorPool.getName());
    }

    public interface StorPoolDfnApi
    {
        UUID getUuid();
        String getName();
        Map<String, String> getProps();
    }

    public interface InitMaps
    {
        Map<NodeName, StorPool> getStorPoolMap();
    }
}
