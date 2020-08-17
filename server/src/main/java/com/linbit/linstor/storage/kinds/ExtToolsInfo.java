package com.linbit.linstor.storage.kinds;

import java.util.ArrayList;
import java.util.List;

public class ExtToolsInfo
{
    private final ExtTools extTool;
    private boolean isSupported;

    private final Version version;

    private final List<String> notSupportedReasons;

    public ExtToolsInfo(
        ExtTools extToolRef,
        boolean isSupportedRef,
        Integer versionMajorRef,
        Integer versionMinorRef,
        /**
         * versionPath might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not
         * null)
         */
        Integer versionPatchRef,
        List<String> notSupportedReasonsRef
    )
    {
        extTool = extToolRef;
        isSupported = isSupportedRef;
        version = new Version(versionMajorRef, versionMinorRef, versionPatchRef);
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

    public final Version getVersion()
    {
        return version;
    }

    public final Integer getVersionMajor()
    {
        return version.major;
    }

    public final Integer getVersionMinor()
    {
        return version.minor;
    }

    /**
     * versionPath might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not null)
     */
    public final Integer getVersionPatch()
    {
        return version.patch;
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
            sb.append(", version: ").append(version.toString());
        }
        else
        {
            sb.append(", reasons: ").append(notSupportedReasons);
        }
        sb.append("]");
        return sb.toString();
    }

    public boolean hasVersionOrHigher(Version ver)
    {
        return version.greaterOrEqual(ver);
    }

    public static class Version
    {
        private final Integer major;
        private final Integer minor;
        private final Integer patch;

        public Version()
        {
            this(null, null, null);
        }
        public Version(int majRef)
        {
            this(majRef, null, null);
        }

        public Version(int majRef, int minRef)
        {
            this(majRef, minRef, null);
        }

        public Version(Integer majRef, Integer minRef, Integer patchRef)
        {
            major = majRef;
            minor = minRef;
            patch = patchRef;
        }

        /**
         * Returns true if the version of "this" object is greater or equal to the parameter.
         *
         * null-values on either side also fulfill "greater or equal"
         */
        public boolean greaterOrEqual(Version v)
        {
            int cmp = major == null || v.major == null ? 1 : Integer.compare(major, v.major);
            if (cmp == 0)
            {
                cmp = minor == null || v.minor == null ? 1 : Integer.compare(minor, v.minor);
                if (cmp == 0)
                {
                    cmp = patch == null | v.patch == null ? 1 : Integer.compare(patch, v.patch);
                }
            }
            return cmp >= 0;
        }

        @Override
        public String toString()
        {
            StringBuilder sb = new StringBuilder();
            if (major != null)
            {
                sb.append(major);
            }
            if (minor != null)
            {
                sb.append(".").append(minor);
            }
            if (patch != null)
            {
                sb.append(".").append(patch);
            }
            return sb.toString();
        }
    }
}
