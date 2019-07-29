package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

public interface ResourceLayerIdDatabaseDriver
{
    void persist(RscLayerObject rscLayerObject) throws DatabaseException;

    void delete(RscLayerObject rscLayerObject) throws DatabaseException;

    <T extends VlmProviderObject> SingleColumnDatabaseDriver<AbsRscData<T>, RscLayerObject> getParentDriver();

    public interface RscLayerInfo
    {
        NodeName getNodeName();

        ResourceName getResourceName();

        long getId();

        Integer getParentId();

        DeviceLayerKind getKind();

        String getRscSuffix();
    }

}
