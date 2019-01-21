package com.linbit.linstor;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.security.ObjectProtection;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.UUID;

public interface KeyValueStore extends TransactionObject
{
    UUID getUuid();

    ObjectProtection getObjProt();

    KeyValueStoreName getName();

    Props getProps(AccessContext accCtx)
        throws AccessDeniedException;

    boolean isDeleted();

    interface InitMaps
    {
        // currently only a place holder for future maps
    }
}
