package com.linbit.linstor;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class SosReportType
{
    protected boolean success;
    protected final String fileName;
    protected final long timestamp;

    protected SosReportType(String fileNameRef, long timestampRef)
    {
        fileName = fileNameRef;
        timestamp = timestampRef;
        success = false;
    }

    public String getFileName()
    {
        return fileName;
    }

    public long getTimestamp()
    {
        return timestamp;
    }

    public void setSuccess()
    {
        success = true;
    }

    public boolean hasFailed()
    {
        return !success;
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + fileName.hashCode();
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
            eq = Objects.equals(fileName, other.fileName) &&
                Objects.equals(timestamp, other.timestamp);
        }
        return eq;
    }

    public static class SosInfoType extends SosReportType
    {
        private final String info;

        public SosInfoType(String fileNameRef, long timestampRef, String infoRef)
        {
            super(fileNameRef, timestampRef);
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
            result = prime * result + info.hashCode();
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
        private final Supplier<Boolean> shouldExec;

        public SosCommandType(String fileNameRef, long timestampRef, String... commandRef)
        {
            this(fileNameRef, timestampRef, () -> true, commandRef);
        }

        public SosCommandType(
            String fileNameRef,
            long timestampRef,
            Supplier<Boolean> shouldExecRef,
            String... commandRef
        )
        {
            super(fileNameRef, timestampRef);
            shouldExec = shouldExecRef;
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

        public boolean shouldExecute()
        {
            return shouldExec.get();
        }
    }

    public static class SosFileType extends SosReportType
    {
        private final Path path;
        private final boolean copyEnabled;

        public SosFileType(
            String fullPathRef,
            boolean copyEnabledRef,
            long createTimestampRef
        )
        {
            super(fullPathRef, createTimestampRef);
            copyEnabled = copyEnabledRef;
            path = Paths.get(fullPathRef);
        }

        public Path getSourcePath()
        {
            return path;
        }

        public boolean isCopyEnabled()
        {
            return copyEnabled;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = super.hashCode();
            result = prime * result + path.hashCode();
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
