package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;
import com.linbit.linstor.storage.interfaces.layers.State;

public interface LvmProviderObject extends VlmProviderObject
{
    State CREATED = new State(true, true, "Created");
    State FAILED = new State(false, true, "Failed");

    String getVolumeGroup();
}
