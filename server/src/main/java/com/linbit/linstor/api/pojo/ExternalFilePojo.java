package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.UUID;

public class ExternalFilePojo implements Comparable<ExternalFilePojo>
{
    private final @Nullable UUID uuid;
    private final String fileName;
    private final long flags;
    private final @Nullable byte[] content;
    private final @Nullable byte[] contentChecksum;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public ExternalFilePojo(
        @Nullable UUID uuidRef,
        String fileNameRef,
        long flagsRef,
        @Nullable byte[] contentRef,
        @Nullable byte[] contentChecksumRef,
        @Nullable Long fullsyncidRef,
        @Nullable Long updateIdRef
    )
    {
        uuid = uuidRef;
        fileName = fileNameRef;
        flags = flagsRef;
        content = contentRef;
        contentChecksum = contentChecksumRef;
        fullSyncId = fullsyncidRef;
        updateId = updateIdRef;
    }

    public @Nullable UUID getUuid()
    {
        return uuid;
    }

    public String getFileName()
    {
        return fileName;
    }

    public long getFlags()
    {
        return flags;
    }

    public @Nullable byte[] getContent()
    {
        return content;
    }

    public byte[] getContentChecksum()
    {
        return contentChecksum;
    }

    public @Nullable Long getFullSyncId()
    {
        return fullSyncId;
    }

    public @Nullable Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(ExternalFilePojo otherExtFileRef)
    {
        return fileName.compareTo(otherExtFileRef.fileName);
    }
}
