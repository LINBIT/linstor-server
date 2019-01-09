package com.linbit.linstor.storage.interfaces.categories;

import com.linbit.linstor.storage.kinds.DeviceLayerKind;
import com.linbit.linstor.transaction.TransactionObject;

public interface LayerObject extends TransactionObject
{
    DeviceLayerKind getLayerKind();
}
