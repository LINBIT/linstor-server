package com.linbit.drbdmanage;

import com.linbit.TransactionObject;
import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.ObjectProtection;
import com.linbit.drbdmanage.storage.StorageDriver;

/**
 * @author Robert Altnoeder &lt;robert.altnoeder@linbit.com&gt;
 */
public interface StorPool extends TransactionObject
{
    StorPoolName getName();

    ObjectProtection getObjProt();

    StorPoolDefinition getDefinition(AccessContext accCtx);

    StorageDriver getDriver(AccessContext accCtx);

    Props getConfiguration(AccessContext accCtx);
}
