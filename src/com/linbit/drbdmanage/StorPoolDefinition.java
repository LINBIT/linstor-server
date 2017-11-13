package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.security.ObjectProtection;

import java.sql.SQLException;
import java.util.Iterator;
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

    public Props getConfiguration(AccessContext accCtx)
        throws AccessDeniedException;

    public void delete(AccessContext accCtx)
        throws AccessDeniedException, SQLException;


}
