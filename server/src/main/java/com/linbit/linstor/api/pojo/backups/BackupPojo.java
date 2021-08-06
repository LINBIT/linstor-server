package com.linbit.linstor.api.pojo.backups;

import com.linbit.linstor.core.apis.BackupApi;

import javax.annotation.Nullable;

import java.util.Map;

public class BackupPojo implements BackupApi
{
    private final String id;
    private final String rscName;
    private final String snapKey;
    private final String startTime;
    private final Long startTimestamp;
    private final String finishedTime;
    private final Long finishedTimestamp;
    private final String node;
    private final Boolean shipping;
    private final Boolean success;
    private final Boolean restoreable;
    private final Map<Integer, BackupVolumePojo> vlms;
    private final String basedOnId;

    private final BackupS3Pojo s3;

    public BackupPojo(
        String idRef,
        String rscNameRef,
        String snapKeyRef,
        @Nullable String startTimeRef,
        @Nullable Long startTimestampRef,
        @Nullable String finishedTimeRef,
        @Nullable Long finishedTimestampRef,
        @Nullable String nodeRef,
        @Nullable Boolean shippingRef,
        @Nullable Boolean successRef,
        @Nullable Boolean restoreableRef,
        Map<Integer, BackupVolumePojo> vlmsRef,
        @Nullable String basedOnRef,
        BackupS3Pojo s3Ref
    )
    {
        id = idRef;
        rscName = rscNameRef;
        snapKey = snapKeyRef;
        startTime = startTimeRef;
        startTimestamp = startTimestampRef;
        finishedTime = finishedTimeRef;
        finishedTimestamp = finishedTimestampRef;
        node = nodeRef;
        shipping = shippingRef;
        success = successRef;
        restoreable = restoreableRef;
        vlms = vlmsRef;
        basedOnId = basedOnRef;
        s3 = s3Ref;
    }

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public String getSnapKey()
    {
        return snapKey;
    }

    @Override
    public String getResourceName()
    {
        return rscName;
    }

    @Override
    public String getStartTime()
    {
        return startTime;
    }

    @Override
    public Long getStartTimestamp()
    {
        return startTimestamp;
    }

    @Override
    public String getFinishedTime()
    {
        return finishedTime;
    }

    @Override
    public Long getFinishedTimestamp()
    {
        return finishedTimestamp;
    }

    @Override
    public String getOriginNodeName()
    {
        return node;
    }

    @Override
    public Boolean isShipping()
    {
        return shipping;
    }

    @Override
    public Boolean successful()
    {
        return success;
    }

    @Override
    public Boolean isRestoreable()
    {
        return restoreable;
    }

    @Override
    public Map<Integer, BackupVolumePojo> getVlms()
    {
        return vlms;
    }

    @Override
    public String getBasedOnId()
    {
        return basedOnId;
    }

    @Override
    public BackupS3Pojo getS3()
    {
        return s3;
    }

    public static class BackupVolumePojo implements BackupVlmApi
    {
        private final long vlmNr;
        private final String finishTime;
        private final Long finishTimestamp;
        private final BackupVlmS3Pojo s3;

        public BackupVolumePojo(long vlmNrRef, String finishTimeRef, Long finishTimestampRef, BackupVlmS3Pojo s3Ref)
        {
            vlmNr = vlmNrRef;
            finishTime = finishTimeRef;
            finishTimestamp = finishTimestampRef;
            s3 = s3Ref;
        }

        @Override
        public long getVlmNr()
        {
            return vlmNr;
        }

        @Override
        public String getFinishedTime()
        {
            return finishTime;
        }

        @Override
        public Long getFinishedTimestamp()
        {
            return finishTimestamp;
        }

        @Override
        public BackupVlmS3Api getS3()
        {
            return s3;
        }
    }

    public static class BackupS3Pojo implements BackupS3Api
    {
        private final String metaName;

        public BackupS3Pojo(String metaNameRef)
        {
            metaName = metaNameRef;
        }

        @Override
        public String getMetaName()
        {
            return metaName;
        }
    }

    public static class BackupVlmS3Pojo implements BackupVlmS3Api
    {
        private final String s3Key;

        public BackupVlmS3Pojo(String s3KeyRef)
        {
            s3Key = s3KeyRef;
        }

        @Override
        public String getS3Key()
        {
            return s3Key;
        }
    }
}
