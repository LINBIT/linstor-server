package com.linbit.linstor.storage.kinds;

import java.util.ArrayList;
import java.util.List;

public class ExtToolsInfo
{
    private final ExtTools extTool;
    private boolean isSupported;

    private final Integer versionMajor;
    private final Integer versionMinor;
    /**
     * versionPath might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not null)
     */
    private final Integer versionPatch;

    private final List<String> notSupportedReasons;

    public ExtToolsInfo(
        ExtTools extToolRef,
        boolean isSupportedRef,
        Integer versionMajorRef,
        Integer versionMinorRef,
        Integer versionPatchRef,
        List<String> notSupportedReasonsRef
    )
    {
        extTool = extToolRef;
        isSupported = isSupportedRef;
        versionMajor = versionMajorRef;
        versionMinor = versionMinorRef;
        versionPatch = versionPatchRef;
        notSupportedReasons = new ArrayList<>();
        if (notSupportedReasonsRef != null)
        {
            notSupportedReasons.addAll(notSupportedReasonsRef);
        }
    }

    public final ExtTools getTool()
    {
        return extTool;
    }

    public final boolean isSupported()
    {
        return isSupported;
    }

    public final Integer getVersionMajor()
    {
        return versionMajor;
    }

    public final Integer getVersionMinor()
    {
        return versionMinor;
    }

    /**
     * versionPath might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not null)
     */
    public final Integer getVersionPatch()
    {
        return versionPatch;
    }

    public final List<String> getNotSupportedReasons()
    {
        return notSupportedReasons;
    }

    public void setSupported(boolean isSupportedRef)
    {
        isSupported = isSupportedRef;
    }

    public void addUnsupportedReason(String reason)
    {
        isSupported = false;
        notSupportedReasons.add(reason);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("ExtToolsInfo [").append(extTool).append(" is supported: ").append(isSupported);
        if (isSupported)
        {
            sb.append(", version: ").append(versionMajor).append(".").append(versionMinor).append(".")
                .append(versionPatch);
        }
        else
        {
            sb.append(", reasons: ").append(notSupportedReasons);
        }
        sb.append("]");
        return sb.toString();
    }

    public boolean hasVersionOrHigher(int maj, int min, int patch)
    {
        return versionMajor != null && versionMajor >= maj &&
            versionMinor != null && versionMinor >= min &&
            (versionPatch == null || versionPatch >= patch);
    }
}
