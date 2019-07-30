package com.linbit.linstor.core.objects;

import com.linbit.linstor.DbgInstanceUuid;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.transaction.TransactionObject;

import java.util.UUID;

public interface VolumeConnection extends DbgInstanceUuid, TransactionObject
{
    UUID getUuid();

    Volume getSourceVolume(AccessContext accCtx) throws AccessDeniedException;

    Volume getTargetVolume(AccessContext accCtx) throws AccessDeniedException;

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, DatabaseException;
}
