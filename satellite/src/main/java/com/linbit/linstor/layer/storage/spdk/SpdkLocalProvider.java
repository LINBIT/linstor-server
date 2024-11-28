package com.linbit.linstor.layer.storage.spdk;

import com.linbit.extproc.ExtCmd.OutputData;
import com.linbit.linstor.layer.storage.spdk.utils.SpdkLocalCommands;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SpdkLocalProvider extends AbsSpdkProvider<OutputData>
{
    @Inject
    public SpdkLocalProvider(AbsStorageProviderInit superInitRef, SpdkLocalCommands spdkLocalCommandsRef)
    {
        super(
            superInitRef,
            "SPDK",
            DeviceProviderKind.SPDK,
            spdkLocalCommandsRef
        );
    }

    @Override
    public DeviceProviderKind getDeviceProviderKind()
    {
        return DeviceProviderKind.SPDK;
    }
}
