package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.dbdrivers.ControllerDatabaseDriver;
import com.linbit.linstor.storage.interfaces.categories.resource.AbsRscLayerObject;

public interface LayerResourceIdCtrlDatabaseDriver extends LayerResourceIdDatabaseDriver,
    ControllerDatabaseDriver<AbsRscLayerObject<?>, Void, Void>
{
}
