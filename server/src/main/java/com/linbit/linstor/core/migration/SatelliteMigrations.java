package com.linbit.linstor.core.migration;

import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.propscon.ReadOnlyProps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public enum SatelliteMigrations
{
    MIG_MARK_ZFS_CF_SNAPS_FOR_DELETION(1, "2024_11_21_MarkZfsCF_SnapshotsForDeletion");

    private static final SatelliteMigrations[] VALUES_BY_LINSTOR_ORDER;
    /**
     * <p>The controller MUST NOT use this constant or the method {@link #getPropNamespaceFull()}.</p>
     * <p>Example:<br/>
     * <ul><li>Controller with version X uses "Satellite/Migrations/" as a prefix and crates a satellite migration
     *         property.</li>
     *     <li>Controller updates from X to Y where the prefix changes to i.e. "Stlt/Migrations/". The controller
     *         with version Y could end up having properties using both prefixes.</li>
     * </ul>
     * If a different cluster jumps over version X (i.e. from W to Y) this second controller will only have the new
     * prefix.</p>
     *<p>That means of course that if the controller with version X migrates to Y (where this prefix changes), the
     * controller-migration from X to Y needs to make sure to update all old "Satellite/Migrations/" to the new
     * "Stlt/Migrations/" (in this example).</p>
     * <p>The satellite however is expected to use this prefix, including the method  {@link #getPropNamespaceFull()}</p>
     */
    public static final String NAMESPC_MIGRATION = "Satellite/Migrations/";

    private final int order;
    private final String propNamespace;

    static
    {
        SatelliteMigrations[] enumValues = values();
        Arrays.sort(enumValues, (e1, e2) -> Integer.compare(e1.order, e2.order));
        VALUES_BY_LINSTOR_ORDER = enumValues;
    }

    public static SatelliteMigrations[] valuesByLinstorOrder()
    {
        return VALUES_BY_LINSTOR_ORDER;
    }

    /**
     * Returns a list of {@link SatelliteMigrations} in the order they should be applied. This method will skip
     * migrations that are not found the given ReadOnlyProps. The returned list might be empty.
     */
    public static List<SatelliteMigrations> getMigrationsToApply(ReadOnlyProps propsRef)
    {
        List<SatelliteMigrations> ret = new ArrayList<>();

        @Nullable ReadOnlyProps allMigNs = propsRef.getNamespace(NAMESPC_MIGRATION);
        if (allMigNs != null)
        {
            for (SatelliteMigrations stltMig : VALUES_BY_LINSTOR_ORDER)
            {
                @Nullable ReadOnlyProps singleMigNs = allMigNs.getNamespace(stltMig.propNamespace);
                if (singleMigNs != null)
                {
                    ret.add(stltMig);
                }
            }
        }

        return ret;
    }

    SatelliteMigrations(int orderRef, String propNamespaceRef)
    {
        order = orderRef;
        propNamespace = propNamespaceRef;
    }

    public String getPropNamespace()
    {
        return propNamespace;
    }

    /**
     * <p>The controller MUST NOT use this or the constant {@link #NAMESPC_MIGRATION}.</p>
     *
     * See {@link #NAMESPC_MIGRATION}
     */
    public String getPropNamespaceFull()
    {
        return NAMESPC_MIGRATION + propNamespace;
    }
}
