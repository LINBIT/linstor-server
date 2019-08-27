package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.SingleColumnDatabaseDriver;
import com.linbit.linstor.core.identifier.NodeName;
import com.linbit.linstor.core.identifier.ResourceName;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.storage.AbsRscData;
import com.linbit.linstor.storage.interfaces.categories.resource.RscLayerObject;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;

import java.util.List;

public interface ResourceLayerIdDatabaseDriver
{
    void persist(RscLayerObject rscLayerObject) throws DatabaseException;

    void delete(RscLayerObject rscLayerObject) throws DatabaseException;

    <T extends VlmProviderObject> SingleColumnDatabaseDriver<AbsRscData<T>, RscLayerObject> getParentDriver();

    public interface RscLayerInfo
    {
        NodeName getNodeName();

        ResourceName getResourceName();

        int getId();

        Integer getParentId();

        DeviceLayerKind getKind();

        String getRscSuffix();
    }

    /*
     * Methods only needed for loading
     */
    List<? extends RscLayerInfo> loadAllResourceIds() throws DatabaseException;

}
