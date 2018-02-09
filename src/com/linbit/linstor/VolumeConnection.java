package com.linbit.linstor;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public interface VolumeConnection extends DbgInstanceUuid
{
    UUID getUuid();

    Volume getSourceVolume(AccessContext accCtx) throws AccessDeniedException;

    Volume getTargetVolume(AccessContext accCtx) throws AccessDeniedException;

    Props getProps(AccessContext accCtx) throws AccessDeniedException;

    void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;
}
