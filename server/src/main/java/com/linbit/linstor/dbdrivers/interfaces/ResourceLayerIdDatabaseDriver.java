package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.NodeName;
import com.linbit.linstor.ResourceName;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.sql.SQLException;

public interface ResourceLayerIdDatabaseDriver
{
    void persist(RscLayerObject rscLayerObject) throws SQLException;

    void delete(RscLayerObject rscLayerObject) throws SQLException;

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
