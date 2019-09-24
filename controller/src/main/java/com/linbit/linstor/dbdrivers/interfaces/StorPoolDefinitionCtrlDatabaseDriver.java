package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.core.objects.StorPoolDefinition;
import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.dbdrivers.DatabaseException;

public interface StorPoolDefinitionCtrlDatabaseDriver extends StorPoolDefinitionDatabaseDriver,
    ControllerDatabaseDriver<StorPoolDefinition, StorPoolDefinition.InitMaps, Void>
{
    @Override
    StorPoolDefinition createDefaultDisklessStorPool() throws DatabaseException;
}
