package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.layers.State;

public interface SfTargetVlmProviderObject<RSC extends AbsResource<RSC>>
    extends SfVlmProviderObject<RSC>
{
    State CREATING = new State(true, false, "Creating");
    State CREATING_TIMEOUT = new State(false, true, "To: Creating");
    State CREATED = new State(true, true, "Created");

    String getVlmOdata();
}
