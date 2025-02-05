package com.linbit.linstor.storage.kinds;

import com.linbit.linstor.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ExtToolsInfo
{
    private final ExtTools extTool;
    private boolean isSupported;

    private final Version version;

    private final @Nullable List<String> notSupportedReasons;

    public ExtToolsInfo(
        ExtTools extToolRef,
        boolean isSupportedRef,
        @Nullable Integer versionMajorRef,
        @Nullable Integer versionMinorRef,
        /**
         * versionPatch might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not
         * null if the ExtTool is supported)
         */
        @Nullable Integer versionPatchRef,
        @Nullable List<String> notSupportedReasonsRef
    )
    {
        this(
            extToolRef,
            isSupportedRef,
            new Version(versionMajorRef, versionMinorRef, versionPatchRef),
            notSupportedReasonsRef
        );
    }

    public ExtToolsInfo(
        ExtTools extToolRef,
        boolean isSupportedRef,
        Version versionRef,
        @Nullable List<String> notSupportedReasonsRef
    )
    {
        extTool = extToolRef;
        isSupported = isSupportedRef;
        version = versionRef;
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

    public boolean isSupportedAndHasVersionOrHigher(Version versionRef)
    {
        return isSupported && versionRef.greaterOrEqual(versionRef);
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
     * versionPatch might be null even if the ExtTool is supported (versionMajor and versionMinor have to be not null)
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

    public static class Version implements Comparable<Version>
    {
        private final @Nullable Integer major;
        private final @Nullable Integer minor;
        private final @Nullable Integer patch;
        private final @Nullable String additionalInfoSeparator;
        private final @Nullable String additionalInfo; // "rc1" or whatever

        public Version()
        {
            this(null, null, null, null);
        }

        public Version(int majRef)
        {
            this(majRef, null, null, null);
        }

        public Version(int majRef, int minRef)
        {
            this(majRef, minRef, null, null);
        }

        public Version(@Nullable Integer majRef, @Nullable Integer minRef, @Nullable Integer patchRef)
        {
            this(majRef, minRef, patchRef, null);
        }

        public Version(
            @Nullable Integer majRef,
            @Nullable Integer minRef,
            @Nullable Integer patchRef,
            @Nullable String additionalInfoRef
        )
        {
            this(majRef, minRef, patchRef, null, additionalInfoRef);
        }

        public Version(
            @Nullable Integer majRef,
            @Nullable Integer minRef,
            @Nullable Integer patchRef,
            @Nullable String additionalInfoSeparatorRef,
            @Nullable String additionalInfoRef
        )
        {
            major = majRef;
            minor = minRef;
            patch = patchRef;
            additionalInfoSeparator = additionalInfoSeparatorRef;
            additionalInfo = additionalInfoRef;
        }

        public @Nullable Integer getMajor()
        {
            return major;
        }

        public @Nullable Integer getMinor()
        {
            return minor;
        }

        public @Nullable Integer getPatch()
        {
            return patch;
        }

        /**
         * <p>
         * Returns true if the version of "this" object is greater or equal to the parameter.
         * </p>
         * <p>
         * null-values on either side also fulfill "greater or equal".
         * </p>
         * <p>
         * By default, the "additionalInfo" field is not compared. This can be changed with the parameter
         * </p>
         */
        public boolean greaterOrEqual(Version vsnRef)
        {
            return greaterOrEqual(vsnRef, false);
        }

        public boolean greaterOrEqual(Version vsnRef, boolean compareAdditionalInfoRef)
        {
            // DO NOT rely on compareTo method, because of different handling of null values
            int cmp = (major == null || vsnRef.major == null) ? 1 : Integer.compare(major, vsnRef.major);
            if (cmp == 0)
            {
                cmp = minor == null || vsnRef.minor == null ? 1 : Integer.compare(minor, vsnRef.minor);
                if (cmp == 0)
                {
                    cmp = patch == null || vsnRef.patch == null ? 1 : Integer.compare(patch, vsnRef.patch);
                    if (cmp == 0 && compareAdditionalInfoRef)
                    {
                        cmp = additionalInfo == null || vsnRef.additionalInfo == null ?
                            1 :
                            additionalInfo.compareTo(vsnRef.additionalInfo);
                    }
                }
            }
            return cmp >= 0;
        }

        @Override
        public int compareTo(Version vsn)
        {
            int cmp = compare(major, vsn.major); // equals
            if (cmp == 0)
            {
                cmp = compare(minor, vsn.minor);
                if (cmp == 0)
                {
                    cmp = compare(patch, vsn.patch);
                    if (cmp == 0)
                    {
                        boolean localNullOrEmpty = additionalInfo == null || additionalInfo.isEmpty();
                        boolean otherNullOrEmpty = vsn.additionalInfo == null || additionalInfo.isEmpty();

                        if (localNullOrEmpty)
                        {
                            cmp = otherNullOrEmpty ? 0 : -1;
                        }
                        else
                        {
                            cmp = otherNullOrEmpty ? 1 : 0;
                        }
                    }
                }
            }
            return cmp;
        }

        private int compare(Integer v1, Integer v2)
        {
            // null will be sorted before not-null
            int cmp;
            if (Objects.equals(v1, v2))
            {
                cmp = 0;
            }
            else if (v1 == null && v2 != null)
            {
                cmp = -1;
            }
            else if (v1 != null && v2 == null)
            {
                cmp = 1;
            }
            else
            {
                cmp = Integer.compare(v1, v2);
            }
            return cmp;
        }

        private int compare(String v1, String v2)
        {
            // null will be sorted before not-null
            int cmp;
            if (Objects.equals(v1, v2))
            {
                cmp = 0;
            }
            else if (v1 == null && v2 != null)
            {
                cmp = -1;
            }
            else if (v1 != null && v2 == null)
            {
                cmp = 1;
            }
            else
            {
                cmp = v1.compareTo(v2);
            }
            return cmp;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((major == null) ? 0 : major.hashCode());
            result = prime * result + ((minor == null) ? 0 : minor.hashCode());
            result = prime * result + ((patch == null) ? 0 : patch.hashCode());
            result = prime * result + ((additionalInfoSeparator == null) ? 0 : additionalInfoSeparator.hashCode());
            result = prime * result + ((additionalInfo == null) ? 0 : additionalInfo.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj)
            {
                return true;
            }
            if (obj == null)
            {
                return false;
            }
            if (getClass() != obj.getClass())
            {
                return false;
            }
            Version other = (Version) obj;
            return Objects.equals(major, other.major) &&
                Objects.equals(minor, other.minor) &&
                Objects.equals(patch, other.patch) &&
                Objects.equals(additionalInfoSeparator, other.additionalInfoSeparator) &&
                Objects.equals(additionalInfo, other.additionalInfo);
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
            if (additionalInfo != null)
            {
                if (additionalInfoSeparator != null)
                {
                    sb.append(additionalInfoSeparator);
                }
                sb.append(additionalInfo);
            }
            return sb.toString();
        }
    }
}
