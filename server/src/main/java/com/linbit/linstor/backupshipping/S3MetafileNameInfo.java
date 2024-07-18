package com.linbit.linstor.backupshipping;

import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class S3MetafileNameInfo
{
    private static final Pattern META_FILE_PATTERN = Pattern.compile(
        "^(?<rscName>[a-zA-Z0-9_-]{2,48})_(?<backupId>back_[0-9]{8}_[0-9]{6})" +
            // in case of "<backupid>^<snapName>" we want to prioritize this combination instead of matching
            // "^<snapName>" as s3Suffix and leaving the snapName empty.

            // please note that "(?:|...)" behaves differently than "(...)?" since the former prioritizes the empty
            // group whereas the latter prioritizes a filled group
            "(?:|(?<s3Suffix>:?.+?))(?<snapName>\\^.*)?\\.meta$"
    );

    public final String rscName;
    public final String backupId;
    public final LocalDateTime backupTime;
    public final String s3Suffix;
    public final String snapName;

    public S3MetafileNameInfo(String rscNameRef, LocalDateTime backupTimeRef, String s3SuffixRef, String snapNameRef)
    {
        rscName = rscNameRef;
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

    public S3MetafileNameInfo(String raw) throws ParseException
    {
        Matcher matcher = META_FILE_PATTERN.matcher(raw);
        if (!matcher.matches())
        {
            throw new ParseException("Failed to parse " + raw + " as S3 backup meta file", 0);
        }

        rscName = matcher.group("rscName");
        backupId = matcher.group("backupId");
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
        return toFullBackupId() + ".meta";
    }

    public String toFullBackupId()
    {
        String result = rscName + "_" + backupId + s3Suffix;
        if (!snapName.isEmpty() && !backupId.equals(snapName))
        {
            result += BackupConsts.SNAP_NAME_SEPARATOR + snapName;
        }
        return result;
    }
}
