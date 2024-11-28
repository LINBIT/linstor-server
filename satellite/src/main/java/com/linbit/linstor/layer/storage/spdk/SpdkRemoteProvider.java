package com.linbit.linstor.layer.storage.spdk;

import com.linbit.linstor.core.pojos.LocalPropsChangePojo;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkRemoteCommands;
import com.linbit.linstor.propscon.ReadOnlyProps;
import com.linbit.linstor.security.AccessDeniedException;
import com.linbit.linstor.storage.StorageException;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.fasterxml.jackson.databind.JsonNode;

@Singleton
public class SpdkRemoteProvider extends AbsSpdkProvider<JsonNode>
{
    @Inject
    public SpdkRemoteProvider(AbsStorageProviderInit superInitRef, SpdkRemoteCommands spdkCommandsRef)
    {
        super(
            superInitRef,
            "RemoteSPDK",
            DeviceProviderKind.REMOTE_SPDK,
            spdkCommandsRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.REMOTE_SPDK;
    }

    @Override
    public LocalPropsChangePojo setLocalNodeProps(ReadOnlyProps localNodePropsRef)
        throws StorageException, AccessDeniedException
    {
        LocalPropsChangePojo changes = super.setLocalNodeProps(localNodePropsRef);
        ((SpdkRemoteCommands) spdkCommands).setLocalNodeProps(localNodePropsRef);
        return changes;
    }
}
