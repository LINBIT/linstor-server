package com.linbit.linstor.layer.storage.ebs;

import com.linbit.extproc.ExtCmd;
import com.linbit.linstor.layer.storage.utils.LsBlkUtils;
import com.linbit.linstor.storage.LsBlkEntry;
import com.linbit.linstor.storage.StorageException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;

public class EbsProviderUtils
{
    private static final int ONE_SECOND = 1000;

    public static HashMap<String, LsBlkEntry> getEbsInfo(ExtCmd extCmd) throws StorageException
    {
        HashMap<String, LsBlkEntry> ret = new HashMap<>();
        List<LsBlkEntry> lsblk = LsBlkUtils.lsblk(extCmd);

        for (LsBlkEntry entry : lsblk)
        {
            ret.put(entry.getName(), entry);
        }
        return ret;
    }

    public static void waitUntilVolumeHasState(
        AmazonEC2 client,
        String ebsVlmId,
        String expectedTargetState,
        String... expectedTransitionState
    )
        throws StorageException
    {
        HashSet<String> expectedTransitionStateSet = new HashSet<>(Arrays.asList(expectedTransitionState));
        boolean targetStateReached = false;
        while (!targetStateReached)
        {
            DescribeVolumesRequest describeVolumesRequest = new DescribeVolumesRequest();
            describeVolumesRequest.withVolumeIds(ebsVlmId);

            DescribeVolumesResult describeVolumes = client.describeVolumes(describeVolumesRequest);

            if (describeVolumes.getVolumes().size() != 1)
            {
                throw new StorageException(
                    "Unexpected volume count: " + describeVolumes.getVolumes().size() + ". " +
                        describeVolumes.getVolumes()
                );
            }
            String state = describeVolumes.getVolumes().get(0).getState();
            targetStateReached = state.equals(expectedTargetState);

            if (!targetStateReached)
            {
                if (!expectedTransitionStateSet.contains(state))
                {
                    throw new StorageException("Unexpected state: " + state);
                }
                try
                {
                    Thread.sleep(ONE_SECOND);
                }
                catch (InterruptedException exc)
                {
                    exc.printStackTrace();
                }
            }
        }
    }
}
