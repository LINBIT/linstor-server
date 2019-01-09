package com.linbit.linstor.storage.layer.provider.lvm;

import com.linbit.linstor.Volume;
import com.linbit.linstor.storage.kinds.DeviceProviderKind;
import com.linbit.linstor.storage.layer.provider.StorageRscData;
import com.linbit.linstor.storage.utils.LvmUtils.LvsInfo;
import com.linbit.linstor.transaction.TransactionMgr;

import javax.inject.Provider;

import java.util.Arrays;

public class LvmThinData extends LvmData
{
    transient String thinPool;

    public LvmThinData(
        Volume vlm,
        StorageRscData rscData,
        Provider<TransactionMgr> transMgrProvider
    )
    {
        super(vlm, rscData, transMgrProvider);

        transObjs = Arrays.asList(
            // FIXME: DevMgrRework: fill transObjs
        );
    }

    @Override
    public DeviceProviderKind getProviderKind()
    {
        return DeviceProviderKind.LVM_THIN;
    }

    @Override
    void updateInfo(LvsInfo info)
    {
        super.updateInfo(info);
        thinPool = info.thinPool;
    }
}
