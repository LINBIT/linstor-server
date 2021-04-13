package com.linbit.linstor.api.pojo;

import java.util.UUID;

public class ExternalFilePojo implements Comparable<ExternalFilePojo>
{
    private final UUID uuid;
    private final String fileName;
    private final long flags;
    private final byte[] content;
    private final byte[] contentChecksum;
    private final Long fullSyncId;
    private final Long updateId;

    public ExternalFilePojo(
        UUID uuidRef,
        String fileNameRef,
        long flagsRef,
        byte[] contentRef,
        byte[] contentChecksumRef,
        Long fullsyncidRef,
        Long updateIdRef
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

    public UUID getUuid()
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

    public byte[] getContent()
    {
        return content;
    }

    public byte[] getContentChecksum()
    {
        return contentChecksum;
    }

    public Long getFullSyncId()
    {
        return fullSyncId;
    }

    public Long getUpdateId()
    {
        return updateId;
    }

    @Override
    public int compareTo(ExternalFilePojo otherExtFileRef)
    {
        return fileName.compareTo(otherExtFileRef.fileName);
    }
}
