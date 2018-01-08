package com.linbit.linstor;

import java.sql.SQLException;
import java.util.UUID;

import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.security.AccessDeniedException;

public interface VolumeConnection extends DbgInstanceUuid
{
    public UUID getUuid();

    public Volume getSourceVolume(AccessContext accCtx) throws AccessDeniedException;

    public Volume getTargetVolume(AccessContext accCtx) throws AccessDeniedException;

    public Props getProps(AccessContext accCtx) throws AccessDeniedException;

    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException;
}
