package com.linbit.linstor.layer.storage;

import com.linbit.linstor.core.objects.StorPool;
import com.linbit.linstor.storage.StorageException;

import javax.annotation.Nullable;


public interface ProbeVlmStorageProvider
{
    long DFLT_PROBE_VLM_SIZE_KIB = 1024;

    @Nullable
    String createTmpProbeVlm(StorPool storPoolRef)
        throws StorageException;

    void deleteTmpProbeVlm(StorPool storPoolRef)
        throws StorageException;
}
