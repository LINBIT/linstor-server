package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic.Result;
import com.linbit.linstor.dbcp.migration.Migration_2025_12_31_NewUpTodateMechanic.VlmDfnKey;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

@EtcdMigration(
    description = "Set existing VlmDfns as DRBD_INITIALIZED",
    version = 71
)
public class Migration_44_NewUpToDateMechanic extends BaseEtcdMigration
{
    private static final String TBL_PROPS = "PROPS_CONTAINERS/";

    private static final String TBL_VLM_DFNS = "VOLUME_DEFINITIONS/";
    private static final String CLM_VD_FLAGS = "VLM_FLAGS";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {

        Map<VlmDfnKey, Long /* VlmDfnFlags */> vlmDfns = getVlmDfns(tx, prefix);
        Map<String, Map<String, String>> oldProps = getProps(tx, prefix);
        Map<VlmDfnKey, Result> result = Migration_2025_12_31_NewUpTodateMechanic.getMigrationResult(vlmDfns, oldProps);
        apply(result, tx, prefix);
        deleteObsoleteRscDfnPrimaryOnProp(tx, prefix);
    }

    private Map<VlmDfnKey, Long /* VlmDfnFlags */> getVlmDfns(EtcdTransaction txRef, String prefixRef)
    {
        Map<VlmDfnKey, Long /* VlmDfnFlags */> ret = new HashMap<>();
        final String prefixedDbTblStr = prefixRef + TBL_VLM_DFNS;
        final int prefixedTblKeyLen = prefixedDbTblStr.length();
        final TreeMap<String, String> etcdRscDfnMap = txRef.get(prefixedDbTblStr, true);
        for (Map.Entry<String, String> etcdEntry : etcdRscDfnMap.entrySet())
        {
            final String etcdKey = etcdEntry.getKey();

            final String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            if (combinedPkAndColumn.endsWith("/" + CLM_VD_FLAGS))
            {
                final int idxOfLastSeparator = combinedPkAndColumn.lastIndexOf("/");
                final String combinedPk = combinedPkAndColumn.substring(0, idxOfLastSeparator);

                final String[] pks = combinedPk.split(":");
                final String rscName = pks[0];
                final @Nullable String snapName = pks[1].isBlank() ? null : pks[1];
                final int vlmNr = Integer.parseInt(pks[2]);
                ret.put(new VlmDfnKey(rscName, snapName, vlmNr), Long.parseLong(etcdEntry.getValue()));
            }
        }
        return ret;
    }

    private Map<String, Map<String, String>> getProps(EtcdTransaction txRef, String prefixRef)
    {
        Map<String, Map<String, String>> ret = new HashMap<>();
        final String prefixedDbTblStr = prefixRef + TBL_PROPS;
        final int prefixedTblKeyLen = prefixedDbTblStr.length();
        final TreeMap<String, String> etcdRscDfnMap = txRef.get(prefixedDbTblStr, true);
        for (Map.Entry<String, String> etcdEntry : etcdRscDfnMap.entrySet())
        {
            final String etcdKey = etcdEntry.getKey();

            final String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            final int idxOfLastSeparator = combinedPkAndColumn.lastIndexOf("/");
            final String combinedPk = combinedPkAndColumn.substring(0, idxOfLastSeparator);

            final String[] pks = combinedPk.split(":");
            final String propInstanceName = pks[0];
            final String propKey = pks[1];
            ret.computeIfAbsent(propInstanceName, ignored -> new HashMap<>())
                .put(propKey, etcdEntry.getValue());
        }
        return ret;
    }

    private void apply(Map<VlmDfnKey, Result> migrationResultRef, EtcdTransaction txRef, String prefixRef)
    {
        final String prefixedTblProps = prefixRef + TBL_PROPS;
        final String propSuffix = ":" +
            Migration_2025_12_31_NewUpTodateMechanic.PROP_KEY_NEW_LINSTOR_DRBD_INITIAL_UPTODATE_ON + "/PROP_VALUE";

        final String prefixedTblVlmDfn = prefixRef + TBL_VLM_DFNS;

        for (Map.Entry<VlmDfnKey, Result> entry : migrationResultRef.entrySet())
        {
            VlmDfnKey vlmDfnKey = entry.getKey();
            Result result = entry.getValue();

            txRef.put(prefixedTblProps + result.vlmDfnPropsInstance + propSuffix, result.winningNodeNamePropValue);
            txRef.put(
                prefixedTblVlmDfn +
                    vlmDfnKey.rscName + ":" +
                    (vlmDfnKey.snapName == null ? "" : vlmDfnKey.snapName) + ":" +
                    vlmDfnKey.vlmNr + "/" + CLM_VD_FLAGS,
                result.updatedFlags + ""
            );
        }
    }

    private void deleteObsoleteRscDfnPrimaryOnProp(EtcdTransaction txRef, String prefixRef)
    {
        final String prefixedDbTblStr = prefixRef + TBL_PROPS;
        final int prefixedTblKeyLen = prefixedDbTblStr.length();
        final TreeMap<String, String> etcdRscDfnMap = txRef.get(prefixedDbTblStr, true);
        for (Map.Entry<String, String> etcdEntry : etcdRscDfnMap.entrySet())
        {
            final String etcdKey = etcdEntry.getKey();

            final String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            final int idxOfLastSeparator = combinedPkAndColumn.lastIndexOf("/");
            final String combinedPk = combinedPkAndColumn.substring(0, idxOfLastSeparator);

            final String[] pks = combinedPk.split(":");
            final String propKey = pks[1];
            if (propKey.equals(Migration_2025_12_31_NewUpTodateMechanic.PROP_KEY_OLD_DRBD_PRIMARY_SET_ON))
            {
                txRef.delete(etcdKey);
            }
        }
    }
}
