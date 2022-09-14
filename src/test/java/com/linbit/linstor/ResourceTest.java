package com.linbit.linstor;

import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.core.objects.Resource;

import java.util.List;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 * @author rpeinthor
 */
public class ResourceTest
{
    @Test
    public void testAllFlags()
    {
        final long mask = Resource.Flags.CLEAN.flagValue |
            Resource.Flags.DELETE.flagValue |
            Resource.Flags.DISK_ADD_REQUESTED.flagValue |
            Resource.Flags.DISK_ADDING.flagValue |
            Resource.Flags.DISK_REMOVE_REQUESTED.flagValue |
            Resource.Flags.DISK_REMOVING.flagValue |
            Resource.Flags.DRBD_DISKLESS.flagValue |
            Resource.Flags.NVME_INITIATOR.flagValue |
            Resource.Flags.TIE_BREAKER.flagValue |
            Resource.Flags.INACTIVE.flagValue |
            Resource.Flags.INACTIVE_PERMANENTLY.flagValue |
            Resource.Flags.REACTIVATE.flagValue |
            Resource.Flags.BACKUP_RESTORE.flagValue |
            Resource.Flags.EVICTED.flagValue |
            Resource.Flags.INACTIVE_BEFORE_EVICTION.flagValue |
            Resource.Flags.RESTORE_FROM_SNAPSHOT.flagValue |
            Resource.Flags.EVACUATE.flagValue |
            Resource.Flags.DRBD_DELETE.flagValue |
            Resource.Flags.INACTIVATING.flagValue |
            Resource.Flags.EBS_INITIATOR.flagValue |
            Resource.Flags.AUTO_DISKFUL.flagValue;
        List<String> strList = Resource.Flags.toStringList(mask);
        assertEquals(Resource.Flags.values().length, strList.size());

        assertArrayEquals(
            new String[]{
                ApiConsts.FLAG_CLEAN,
                ApiConsts.FLAG_DELETE,
                ApiConsts.FLAG_DISKLESS,
                ApiConsts.FLAG_DISK_ADD_REQUESTED,
                ApiConsts.FLAG_DISK_ADDING,
                ApiConsts.FLAG_DISK_REMOVE_REQUESTED,
                ApiConsts.FLAG_DISK_REMOVING,
                ApiConsts.FLAG_DRBD_DISKLESS,
                ApiConsts.FLAG_TIE_BREAKER,
                ApiConsts.FLAG_NVME_INITIATOR,
                ApiConsts.FLAG_RSC_INACTIVE,
                "REACTIVATE", // internal
                "INACTIVE_PERMANENTLY", // internal
                "BACKUP_RESTORE", // internal,
                ApiConsts.FLAG_EVICTED,
                "INACTIVE_BEFORE_EVICTION", // internal
                "RESTORE_FROM_SNAPSHOT", // internal
                ApiConsts.FLAG_EVACUATE,
                "DRBD_DELETE", // internal
                "INACTIVATING", // internal
                ApiConsts.FLAG_EBS_INITIATOR,
                "AUTO_DISKFUL" // internal
            },
            strList.toArray()
        );
        assertEquals(mask, Resource.Flags.fromStringList(strList));
    }

    @Test
    public void testFlags()
    {
        {
            final long mask = Resource.Flags.CLEAN.flagValue |
                Resource.Flags.DRBD_DISKLESS.flagValue;
            List<String> strList = Resource.Flags.toStringList(mask);

            assertArrayEquals(
                new String[]
                {
                    ApiConsts.FLAG_CLEAN, ApiConsts.FLAG_DISKLESS, ApiConsts.FLAG_DRBD_DISKLESS
                },
                strList.toArray()
            );
            assertEquals(mask, Resource.Flags.fromStringList(strList));
        }

        {
            final long mask = 0;
            List<String> strList = Resource.Flags.toStringList(mask);

            assertArrayEquals(
                new String[]
                {},
                strList.toArray()
            );
            assertEquals(mask, Resource.Flags.fromStringList(strList));
        }
    }
}
