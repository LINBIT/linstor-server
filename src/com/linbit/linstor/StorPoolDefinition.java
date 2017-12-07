package com.linbit.linstor;

import com.linbit.TransactionObject;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Definition of a storage pool
 *
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPoolDefinition extends TransactionObject
{
    public UUID getUuid();

    public ObjectProtection getObjProt();

    public StorPoolName getName();

    public Iterator<StorPool> iterateStorPools(AccessContext accCtx)
        throws AccessDeniedException;

    public Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;

    public StorPoolDfnApi getApiData(AccessContext accCtx) throws AccessDeniedException;

    public interface StorPoolDfnApi
    {
        UUID getUuid();
        String getName();
        Map<String, String> getProps();
    }
}
