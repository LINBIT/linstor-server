package com.linbit.drbdmanage;

import java.sql.SQLException;
import java.util.Collection;
import java.util.UUID;

import com.linbit.drbdmanage.propscon.Props;
import com.linbit.drbdmanage.security.AccessContext;
import com.linbit.drbdmanage.security.AccessDeniedException;
import com.linbit.drbdmanage.storage.StorageDriver;
import com.linbit.drbdmanage.storage.StorageException;

public class SatelliteDummyStorPoolData extends StorPoolData
{
    public SatelliteDummyStorPoolData()
    {
        super();
    }

    private static final String EXC_MSG =
        "This is a dummy remote storPool. This instance's getter must not be called";


    @Override
    public UUID getUuid()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorPoolName getName()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public Node getNode()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorPoolDefinition getDefinition(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public StorageDriver getDriver(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public String getDriverName()
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public Props getConfiguration(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public void putVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        // no-op
    }

    @Override
    public void removeVolume(AccessContext accCtx, Volume volume) throws AccessDeniedException
    {
        // no-op
    }

    @Override
    public Collection<Volume> getVolumes(AccessContext accCtx) throws AccessDeniedException
    {
        throw new UnsupportedOperationException(EXC_MSG);
    }

    @Override
    public void reconfigureStorageDriver() throws StorageException
    {
        // no-op
    }

    @Override
    public void delete(AccessContext accCtx) throws AccessDeniedException, SQLException
    {
        // no-op
    }
}
