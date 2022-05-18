package com.linbit.linstor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;

public abstract class SosReportType
{
    protected final String relativeFileName;
    protected final long timestamp;

    protected SosReportType(String relativeFileNameRef, long timestampRef)
    {
        relativeFileName = relativeFileNameRef;
        timestamp = timestampRef;
    }

    public String getRelativeFileName()
    {
        return relativeFileName;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((relativeFileName == null) ? 0 : relativeFileName.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        boolean eq = false;
        if (obj instanceof SosReportType)
        {
            SosReportType other = (SosReportType) obj;
            eq = Objects.equals(relativeFileName, other.relativeFileName) &&
                Objects.equals(timestamp, other.timestamp);
        }
        return eq;
    }

    public static class SosInfoType extends SosReportType
    {
        private final String info;

        public SosInfoType(String relativeFileNameRef, long timestampRef, String infoRef)
        {
            super(relativeFileNameRef, timestampRef);
            info = infoRef;
        }

        public String getInfo()
        {
            return info;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((info == null) ? 0 : info.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj instanceof SosInfoType)
            {
                SosInfoType other = (SosInfoType) obj;
                eq = super.equals(obj) && Objects.equals(info, other.info);
            }
            return eq;
        }
    }

    public static class SosCommandType extends SosReportType
    {
        private final String[] command;

        public SosCommandType(String relativeFileNameRef, long timestampRef, String... commandRef)
        {
            super(relativeFileNameRef, timestampRef);
            command = commandRef;
        }

        public String[] getCommand()
        {
            return command;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + Arrays.hashCode(command);
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj instanceof SosCommandType)
            {
                SosCommandType other = (SosCommandType) obj;
                eq = super.equals(obj) && Arrays.equals(command, other.command);
            }
            return eq;
        }
    }

    public static class SosFileType extends SosReportType
    {
        private final Path path;

        public SosFileType(String relativeFileNameRef, String fullPathRef, long createTimestampRef)
        {
            super(relativeFileNameRef, createTimestampRef);
            path = Paths.get(fullPathRef);
        }

        public Path getPath()
        {
            return path;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + ((path == null) ? 0 : path.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj)
        {
            boolean eq = false;
            if (obj instanceof SosFileType)
            {
                SosFileType other = (SosFileType) obj;
                eq = super.equals(obj) && Objects.equals(path, other.path);
            }
            return eq;
        }
    }
}
