package com.linbit.linstor.api.pojo;

import com.linbit.linstor.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ExternalFilePojo implements Comparable<ExternalFilePojo>
{
    private final @Nullable UUID uuid;
    private final String fileName;
    private final long flags;
    private final @Nullable byte[] content;
    private final @Nullable byte[] contentChecksum;
    private final List<String> altSuffixes;
    private final @Nullable Long fullSyncId;
    private final @Nullable Long updateId;

    public ExternalFilePojo(
        @Nullable UUID uuidRef,
        String fileNameRef,
        long flagsRef,
        @Nullable byte[] contentRef,
        @Nullable byte[] contentChecksumRef,
        List<String> altSuffixesRef,
        @Nullable Long fullsyncidRef,
        @Nullable Long updateIdRef
    )
    {
        uuid = uuidRef;
        fileName = fileNameRef;
        flags = flagsRef;
        content = contentRef;
        contentChecksum = contentChecksumRef;
        altSuffixes = altSuffixesRef == null ? Collections.emptyList() : altSuffixesRef;
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

    public List<String> getAltSuffixes()
    {
        return altSuffixes;
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
