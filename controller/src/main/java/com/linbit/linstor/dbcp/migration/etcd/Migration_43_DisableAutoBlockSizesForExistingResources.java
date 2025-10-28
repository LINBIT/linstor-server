package com.linbit.linstor.dbcp.migration.etcd;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.dbcp.migration.Migration_2025_10_28_DisableAutoBlockSizesForExistingResources;
import com.linbit.linstor.dbdrivers.DatabaseException;
import com.linbit.linstor.transaction.EtcdTransaction;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

@EtcdMigration(
    description = "Disable auto-block-size for existing resources",
    version = 70
)
public class Migration_43_DisableAutoBlockSizesForExistingResources extends BaseEtcdMigration
{
    private static final String TBL_RSC_DFN = Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.TBL_RSC_DFN +
        "/";

    @Override
    public void migrate(EtcdTransaction tx, final String prefix) throws Exception
    {
        Collection<String> rscNames = getRscNames(tx, prefix);
        HashMap<String, HashMap<String, String>> blockSizeProps = Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.getPropsToInsert(
            rscNames
        );

        setProps(tx, prefix, blockSizeProps);
    }

    private Collection<String> getRscNames(EtcdTransaction tx, String prefix)
    {
        HashSet<String> ret = new HashSet<>();
        final String prefixedDbTableStr = prefix + TBL_RSC_DFN;
        final int prefixedTblKeyLen = prefixedDbTableStr.length();
        final TreeMap<String, String> etcdRscDfnMap = tx.get(prefixedDbTableStr, true);
        for (Map.Entry<String, String> etcdEntry : etcdRscDfnMap.entrySet())
        {
            final String etcdKey = etcdEntry.getKey();

            final String combinedPkAndColumn = etcdKey.substring(prefixedTblKeyLen);
            final int idxOfLastSeparator = combinedPkAndColumn.lastIndexOf("/");
            final String combinedPk = combinedPkAndColumn.substring(0, idxOfLastSeparator);

            final String[] pks = combinedPk.split(":");

            ret.add(pks[0]);
        }
        return ret;
    }

    private void setProps(
        EtcdTransaction txRef,
        String prefixRef,
        HashMap<String, HashMap<String, String>> blockSizePropsRef
    )
    {
        final String etcdKeyPrefix = prefixRef + Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.TBL_PROPS + "/";
        for (Map.Entry<String, HashMap<String, String>> outerEntry : blockSizePropsRef.entrySet())
        {
            final String instanceName = outerEntry.getKey();
            final String prefixedInstanceName = etcdKeyPrefix + instanceName + ":";
            for (Map.Entry<String, String> innerEntry : outerEntry.getValue().entrySet())
            {
                final String propKey = innerEntry.getKey();
                final String propValue = innerEntry.getValue();

                txRef.put(prefixedInstanceName + propKey, propValue);
            }
        }
    }

    private class LriInfoBuilder
    {
        private final int id;
        private String rscName;
        private String kind;
        private @Nullable Integer parentId;

        public LriInfoBuilder(int idRef)
        {
            id = idRef;
        }

        private Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.LriInfo build() throws DatabaseException
        {
            checkNonNull(rscName, "rscName");
            checkNonNull(kind, "kind");
            return new Migration_2025_10_28_DisableAutoBlockSizesForExistingResources.LriInfo(id, rscName, kind, parentId);
        }

        private void checkNonNull(@Nullable String valueRef, String descriptionRef) throws DatabaseException
        {
            if (valueRef == null)
            {
                throw new DatabaseException("LRI id " + id + " did not have a " + descriptionRef);
            }
        }
    }
}
