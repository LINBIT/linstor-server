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
import com.amazonaws.services.ec2.model.DescribeSnapshotsRequest;
import com.amazonaws.services.ec2.model.DescribeSnapshotsResult;
import com.amazonaws.services.ec2.model.DescribeVolumesRequest;
import com.amazonaws.services.ec2.model.DescribeVolumesResult;

public class EbsProviderUtils
{
    private static final int DFLT_WAIT_TIMEOUT = 60_000;
    private static final int ONE_SECOND = 1000;

    private static final String SNAP_CREATE_STATE_COMPLETED = "completed";
    private static final String SNAP_CREATE_STATE_PENDING = "pending";
    private static final String SNAP_CREATE_STATE_ERROR = "error";

    private EbsProviderUtils()
    {
        // utility class
    }

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
        waitUntilVolumeHasState(
            client,
            DFLT_WAIT_TIMEOUT,
            ebsVlmId,
            expectedTargetState,
            expectedTransitionState
        );
    }

    public static void waitUntilVolumeHasState(
        AmazonEC2 client,
        int waitTimeoutInMs,
        String ebsVlmId,
        String expectedTargetState,
        String... expectedTransitionState
    )
        throws StorageException
    {
        HashSet<String> expectedTransitionStateSet = new HashSet<>(Arrays.asList(expectedTransitionState));
        boolean targetStateReached = false;

        DescribeVolumesRequest describeVolumesRequest = new DescribeVolumesRequest()
            .withVolumeIds(ebsVlmId);

        String currentState = null;
        long start = System.currentTimeMillis();
        while (!targetStateReached && (System.currentTimeMillis() - start <= waitTimeoutInMs))
        {
            DescribeVolumesResult describeVolumes = client.describeVolumes(describeVolumesRequest);

            if (describeVolumes.getVolumes().size() != 1)
            {
                throw new StorageException(
                    "Unexpected volume count: " + describeVolumes.getVolumes().size() + ". " +
                        describeVolumes.getVolumes()
                );
            }
            currentState = describeVolumes.getVolumes().get(0).getState();
            targetStateReached = currentState.equals(expectedTargetState);

            if (!targetStateReached)
            {
                if (!expectedTransitionStateSet.contains(currentState))
                {
                    throw new StorageException("Unexpected state: " + currentState);
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
        if (!targetStateReached)
        {
            throw new StorageException(
                "Did not reach target state within " + waitTimeoutInMs + "ms. Last known state: " + currentState
            );
        }
    }

    public static void waitUntilSnapshotCreated(
        AmazonEC2 client,
        String ebsSnapId
    )
        throws StorageException
    {
        waitUntilSnapshotCreated(
            client,
            DFLT_WAIT_TIMEOUT,
            ebsSnapId
        );
    }

    public static void waitUntilSnapshotCreated(
        AmazonEC2 client,
        int waitTimeoutInMs,
        String ebsSnapId
    )
        throws StorageException
    {
        boolean created = false;
        long start = System.currentTimeMillis();

        DescribeSnapshotsRequest describeSnapshotsRequest = new DescribeSnapshotsRequest().withSnapshotIds(ebsSnapId);
        while (!created && (System.currentTimeMillis() - start <= waitTimeoutInMs))
        {
            DescribeSnapshotsResult describeSnapshots = client.describeSnapshots(describeSnapshotsRequest);

            int snapCount = describeSnapshots.getSnapshots().size();
            if (snapCount > 1)
            {
                throw new StorageException(
                    "Unexpected snapshot count for EBS snapshot id: " + ebsSnapId + ": " + snapCount
                );
            }
            String snapshotState = describeSnapshots.getSnapshots().get(0).getState();

            if (snapshotState.equalsIgnoreCase(SNAP_CREATE_STATE_ERROR))
            {
                throw new StorageException("EBS snapshot has state: '" + snapshotState + "'");
            }

            created = snapCount == 1 &&
                (snapshotState.equalsIgnoreCase(SNAP_CREATE_STATE_COMPLETED) ||
                    snapshotState.equalsIgnoreCase(SNAP_CREATE_STATE_PENDING));

            try
            {
                Thread.sleep(ONE_SECOND);
            }
            catch (InterruptedException exc)
            {
                exc.printStackTrace();
            }
        }
        if (!created)
        {
            throw new StorageException(
                "Snapshot was not created within " + waitTimeoutInMs + "ms"
            );
        }
    }
}
