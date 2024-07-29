package com.linbit.linstor.spacetracking;

import com.linbit.SystemService;
import com.linbit.linstor.annotation.Nullable;
import com.linbit.linstor.core.objects.Node;
import com.linbit.linstor.dbdrivers.DatabaseException;

import java.security.DigestException;
import java.security.NoSuchAlgorithmException;
import java.util.Calendar;

public interface SpaceTrackingService extends SystemService
{
    void receiveSpaceTrackingReport(Node stltNode, long checksum, byte[] msgQryDate, AggregateCapacityInfo newCapInfo);
    void cancelSpaceTrackingReport(Node stltNode);

    String querySpaceReport(@Nullable Calendar startDateCal)
        throws DatabaseException, NoSuchAlgorithmException, DigestException;
}
