package com.linbit.linstor.api.utils;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.api.ApiRcUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbsApiCallTester
{
    public List<Long> retCodes;
    public List<StltConnectingAttempt> expectedSyncConnectingAttempts;
    public List<StltConnectingAttempt> expectedAsyncConnectingAttempts;

    public AbsApiCallTester(
        long objMask,
        long opMask,
        long... expectedRetCodes
    )
    {
        retCodes = new ArrayList<>();
        for (long expectedRetCode : expectedRetCodes)
        {
            long rc = expectedRetCode;
            if ((expectedRetCode & ApiConsts.MASK_NODE) == 0) // MASK_NODE has all objMask bits set
            {
                rc |= objMask;
            }
            if ((expectedRetCode & ApiConsts.MASK_DEL) == 0) // MASK_DEL has all opMask bits set
            {
                rc |= opMask;
            }
            retCodes.add(rc);
        }

        expectedSyncConnectingAttempts = new ArrayList<>();
        expectedAsyncConnectingAttempts = new ArrayList<>();
    }

    public AbsApiCallTester retCode(int index, long retCode)
    {
        retCodes.set(index, retCode);
        return this;
    }

    public AbsApiCallTester addRetCode(long retCode)
    {
        retCodes.add(retCode);
        return this;
    }

    public AbsApiCallTester expectStltConnectingAttempt(boolean async)
    {
        if (async)
        {
            expectedAsyncConnectingAttempts.add(
                new StltConnectingAttempt(null, null, null) // just a dummy entry to be checked
            );
        }
        else
        {
            expectedSyncConnectingAttempts.add(
                new StltConnectingAttempt(null, null, null) // just a dummy entry to be checked
            );
        }
        return this;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (long rc : retCodes)
        {
            ApiRcUtils.appendReadableRetCode(sb, rc);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public abstract ApiCallRc executeApiCall()
        throws Exception;
}
