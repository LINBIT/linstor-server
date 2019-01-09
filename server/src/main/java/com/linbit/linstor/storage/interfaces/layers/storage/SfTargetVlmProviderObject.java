package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.storage.interfaces.layers.State;

public interface SfTargetVlmProviderObject extends SfVlmProviderObject
{
    State CREATING = new State(true, false, "Creating");
    State CREATING_TIMEOUT = new State(false, true, "To: Creating");
    State CREATED = new State(true, true, "Created");

    String getVlmOdata();

    String getStorPoolService();
}
