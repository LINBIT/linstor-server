package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.core.objects.AbsResource;
import com.linbit.linstor.storage.interfaces.categories.resource.VlmProviderObject;

public interface ZfsProviderObject<RSC extends AbsResource<RSC>>
    extends VlmProviderObject<RSC>
{
    String getZPool();

    @Override
    String getIdentifier();
}
