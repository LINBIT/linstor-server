package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3VolumeNameInfo
{
    private static final Pattern BACKUP_VOLUME_PATTERN = Pattern.compile(
        "^(?<rscName>[a-zA-Z0-9_-]{2,48})(?<rscSuffix>\\..+)?_(?<vlmNr>[0-9]{5})_(?<backupId>back_[0-9]{8}_[0-9]{6})" +
            "(?<s3Suffix>:?.+?)?(?<snapName>\\^.*)?$"
    );

    public final String rscName;
    public final String layerSuffix;
    public final int vlmNr;
    public final String backupId;
    public final LocalDateTime backupTime;
    public final String s3Suffix;
    public final String snapName;

    public S3VolumeNameInfo(
        String rscNameRef,
        String layerSuffixRef,
        int vlmNrRef,
        LocalDateTime backupTimeRef,
        String s3SuffixRef,
        String snapNameRef
    )
    {
        rscName = rscNameRef;
        layerSuffix = BackupShippingUtils.defaultEmpty(layerSuffixRef);
        vlmNr = vlmNrRef;
        backupId = BackupConsts.BACKUP_PREFIX + BackupConsts.DATE_FORMAT.format(backupTimeRef);
        backupTime = backupTimeRef;
        s3Suffix = BackupShippingUtils.defaultEmpty(s3SuffixRef);
        if (snapNameRef == null || snapNameRef.isEmpty())
        {
            snapName = backupId;
        }
        else
        {
            snapName = snapNameRef;
        }
    }

    public S3VolumeNameInfo(String raw) throws ParseException
    {
        Matcher matcher = BACKUP_VOLUME_PATTERN.matcher(raw);
        if (!matcher.matches())
        {
            throw new ParseException("Failed to parse " + raw + " as S3 backup meta file", 0);
        }

        rscName = matcher.group("rscName");
        backupId = matcher.group("backupId");
        layerSuffix = BackupShippingUtils.defaultEmpty(matcher.group("rscSuffix"));
        vlmNr = Integer.parseInt(matcher.group("vlmNr"));
        backupTime = BackupConsts.DATE_FORMAT.parse(
            backupId.substring(BackupConsts.BACKUP_PREFIX_LEN),
            LocalDateTime::from
        );
        s3Suffix = BackupShippingUtils.defaultEmpty(matcher.group("s3Suffix"));

        String snapNameRef = BackupShippingUtils.defaultEmpty(matcher.group("snapName"));

        if (snapNameRef.startsWith(BackupConsts.SNAP_NAME_SEPARATOR))
        {
            snapNameRef = snapNameRef.substring(BackupConsts.SNAP_NAME_SEPARATOR_LEN);
        }

        if (snapNameRef.isEmpty())
        {
            snapNameRef = backupId;
        }

        snapName = snapNameRef;
    }

    @Override
    public String toString()
    {
        String result = String.format("%s%s_%05d_%s%s", rscName, layerSuffix, vlmNr, backupId, s3Suffix);
        if (!snapName.isEmpty() && !backupId.equals(snapName))
        {
            result += BackupConsts.SNAP_NAME_SEPARATOR + snapName;
        }
        return result;
    }

}
