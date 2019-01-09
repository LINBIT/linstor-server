package com.linbit.linstor.core.devmgr.helper;

import com.linbit.linstor.Resource;
import com.linbit.linstor.propscon.InvalidKeyException;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.interfaces.categories.RscLayerObject;
import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.utils.Pair;

import java.sql.SQLException;
import java.util.List;

public interface LayerDataConverter
{
    DeviceLayerKind getKind();

    boolean isLayerNeeded(Resource rsc);

    /**
     * Converts the given resource with the possibly null parentRscData. Returns a {@link Pair} where objA
     * is the newly created {@link RscLayerObject} of the current layer and objB is a {@link List} of
     * {@link RscLayerObject}s of objA's children. Thus, objB should be in every case an empty list, so that
     * the next layer's {@link RscLayerObject}(s) can be added to the previous layer's children-list.
     *
     * @param rsc
     * @param parentRscData
     * @return
     * @throws AccessDeniedException
     * @throws InvalidKeyException
     * @throws SQLException
     */
    Pair<RscLayerObject, List<RscLayerObject>> convert(Resource rsc, RscLayerObject parentRscData)
        throws AccessDeniedException, InvalidKeyException, SQLException;
}
