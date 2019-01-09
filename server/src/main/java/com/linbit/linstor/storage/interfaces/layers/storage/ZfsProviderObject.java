package com.linbit.linstor.storage.interfaces.layers.storage;

import com.linbit.linstor.storage.interfaces.categories.VlmProviderObject;

public interface ZfsProviderObject extends VlmProviderObject
{
    String getZPool();

    String getIdentifier();
}
