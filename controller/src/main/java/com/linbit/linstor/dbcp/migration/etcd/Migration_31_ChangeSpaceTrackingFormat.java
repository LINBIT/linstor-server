package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.EtcdTransaction;
import com.linbit.utils.TimeUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@EtcdMigration(
    description = "Change Space Tracking key format",
    version = 58
)
public class Migration_31_ChangeSpaceTrackingFormat extends BaseEtcdMigration
{
    /*
     * Since we want to unify all database drivers, SpaceTracking also need to change:
     * Before, ETCD stored a value like this:
     * "/LINSTOR/SATELLITES_CAPACITY/<stltNodeName>" = "<capacity>,<allocated>,<usable>,<storPoolExcFlag>"
     *
     * This has to change in a few ways: First, we need the "/LINSTOR/" prefix. Second, the 4 values need to be put in
     * the common format:
     * "/LINSTOR/SATELLITE_CAPACITY/<stltNodeName>/CAPACITY" = "<capacity>"
     * "/LINSTOR/SATELLITE_CAPACITY/<stltNodeName>/ALLOCATED" = "<allocated>"
     * "/LINSTOR/SATELLITE_CAPACITY/<stltNodeName>/USABLE" = "<usable>"
     * "/LINSTOR/SATELLITE_CAPACITY/<stltNodeName>/STOR_POOL_EXC_FLAG" = "<storPoolExcFlag>"
     */

    /*
     * Additionally, we also need to add the prefix for the other 2 SpaceTracking tables:
     * "/LINSTOR/TRACKING_DATE/" = "yyyy-mm-dd"
     * "/LINSTOR/SPACE_HISTORY/<entryDate>" = "<value>"
     * ->
     * "/LINSTOR/TRACKING_DATE/ENTRY_DATE" = "<long timestamp>"
     * "/LINSTOR/SPACE_HISTORY/<entryDate>/CAPACITY" = "<value>"
     */

    private static final String TBL_STLT_CAP = "SATELLITES_CAPACITY/";
    private static final String TBL_SPC_HIST = "SPACE_HISTORY/";
    private static final String TBL_TRACK_DATE = "TRACKING_DATE/";

    private static final String P_CGRP_CAPACITY = "capacity";
    private static final String P_CGRP_ALLOCATED = "allocated";
    private static final String P_CGRP_USABLE = "usable";
    private static final String P_CGRP_STOR_POOL_EXC_FLAG = "storPoolExcFlag";

    private static final String CLM_NAME_ALLOCATED = "ALLOCATED";
    private static final String CLM_NAME_CAPACITY = "CAPACITY";
    private static final String CLM_NAME_FAIL_FLAG = "FAIL_FLAG";
    private static final String CLM_NAME_USABLE = "USABLE";

    private static final String CLM_ENTRY_DATE = "ENTRY_DATE";

    private static final String CLM_CAPACITY = "CAPACITY";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        fixSatelliteCapacityKeys(tx, prefix);
        fixSpaceHistoryKeys(tx, prefix);
        fixTrackingDateKey(tx, prefix);
    }

    private void fixSatelliteCapacityKeys(EtcdTransaction tx, final String prefix) throws DatabaseException
    {
        Pattern pattern = Pattern.compile(
            "(?<" + P_CGRP_CAPACITY + ">[^,]+)," +
                "(?<" + P_CGRP_ALLOCATED + ">[^,]+)," +
                "(?<" + P_CGRP_USABLE + ">[^,]+)," +
                "(?<" + P_CGRP_STOR_POOL_EXC_FLAG + ">[^,]+)"
        );
        String stltCapTblWithPrefix = prefix + TBL_STLT_CAP;
        String clmKeyAllocatedFormat = stltCapTblWithPrefix + "%s/" + CLM_NAME_ALLOCATED;
        String clmKeyCapacityFormat = stltCapTblWithPrefix + "%s/" + CLM_NAME_CAPACITY;
        String clmKeyFailFlagFormat = stltCapTblWithPrefix + "%s/" + CLM_NAME_FAIL_FLAG;
        String clmKeyUsableFormat = stltCapTblWithPrefix + "%s/" + CLM_NAME_USABLE;

        TreeMap<String, String> data = tx.get(stltCapTblWithPrefix);

        for (Entry<String, String> entry : data.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();

            String stltNodeName = oldFullEtcdKey.substring(stltCapTblWithPrefix.length());
            Matcher matcher = pattern.matcher(entry.getValue());
            if (!matcher.matches())
            {
                throw new DatabaseException("Could not match: " + entry.getValue());
            }
            String capacity = matcher.group(P_CGRP_CAPACITY);
            String allocated = matcher.group(P_CGRP_ALLOCATED);
            String usable = matcher.group(P_CGRP_USABLE);
            String storPoolExcFlag = matcher.group(P_CGRP_STOR_POOL_EXC_FLAG);

            tx.delete(oldFullEtcdKey, false);

            tx.put(String.format(clmKeyAllocatedFormat, stltNodeName), allocated);
            tx.put(String.format(clmKeyCapacityFormat, stltNodeName), capacity);
            tx.put(String.format(clmKeyUsableFormat, stltNodeName), usable);
            tx.put(String.format(clmKeyFailFlagFormat, stltNodeName), storPoolExcFlag);
        }
    }


    /*
     * Additionally, we also need to add the prefix for the other 2 SpaceTracking tables:
     * "TRACKING_DATE/" = "yyyy-mm-dd"
     * "SPACE_HISTORY/<entryDate>" = "<value>"
     * ->
     * "/LINSTOR/TRACKING_DATE/ENTRY_DATE" = "<value"
     * "/LINSTOR/SPACE_HISTORY/<entryDate>/CAPACITY" = "<value>"
     */
    private void fixSpaceHistoryKeys(EtcdTransaction tx, String prefix) throws DatabaseException
    {
        String tblWithPrefix = prefix + TBL_SPC_HIST;
        TreeMap<String, String> data = tx.get(tblWithPrefix);

        for (Entry<String, String> entry : data.entrySet())
        {
            String oldFullEtcdKey = entry.getKey();
            String entryDate = oldFullEtcdKey.substring(tblWithPrefix.length());
            try
            {
                LocalDateTime parsedDate;
                parsedDate = TimeUtils.DTF_NO_TIME.parse(entryDate, LocalDateTime::from);

                tx.delete(oldFullEtcdKey);
                tx.put(
                    tblWithPrefix + Long.toString(TimeUtils.getEpochMillis(parsedDate)) +
                        "/" + CLM_CAPACITY,
                    entry.getValue()
                );
            }
            catch (DateTimeParseException exc)
            {
                throw new DatabaseException("Unexpected date format: " + entryDate, exc);
            }
        }
    }

    private void fixTrackingDateKey(EtcdTransaction tx, String prefix) throws DatabaseException
    {
        String tblWithPrefix = prefix + TBL_TRACK_DATE;
        TreeMap<String, String> data = tx.get(tblWithPrefix);

        for (Entry<String, String> entry : data.entrySet())
        {
            // should be only zero or one entry
            try
            {
                LocalDateTime date = TimeUtils.DTF_NO_TIME.parse(entry.getValue(), LocalDateTime::from);
                tx.delete(entry.getKey());
                tx.put(
                    tblWithPrefix + CLM_ENTRY_DATE,
                    Long.toString(TimeUtils.getEpochMillis(date))
                );
            }
            catch (DateTimeParseException exc)
            {
                throw new DatabaseException("Unexpected date format: " + entry.getValue(), exc);
            }
        }
    }
}
