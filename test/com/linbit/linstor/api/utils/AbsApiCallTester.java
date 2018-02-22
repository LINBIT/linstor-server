package com.linbit.linstor.api.utils;

import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.ApiConsts;
import com.linbit.linstor.netcom.Peer;
import com.linbit.linstor.security.AccessContext;
import com.linbit.linstor.testclient.ApiRCUtils;

public abstract class AbsApiCallTester
{
    public AccessContext accCtx;
    public Peer peer;
    public List<Long> retCodes;
    public List<StltConnectingAttempt> expectedConnectingAttempts;

    public AbsApiCallTester(
        AccessContext accCtxRef,
        Peer peerRef,
        long objMask,
        long opMask,
        long... expectedRetCodes
    )
    {
        this.accCtx = accCtxRef;
        this.peer = peerRef;

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

        expectedConnectingAttempts = new ArrayList<>();
    }

    public AbsApiCallTester accCtx(AccessContext accCtxRef)
    {
        this.accCtx = accCtxRef;
        return this;
    }

    public AbsApiCallTester peer(Peer peerRef)
    {
        this.peer = peerRef;
        return this;
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

    public AbsApiCallTester expectStltConnectingAttempt()
    {
        expectedConnectingAttempts.add(
            new StltConnectingAttempt(null, null, null) // just a dummy entry to be checked
        );
        return this;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        for (long rc : retCodes)
        {
            ApiRCUtils.appendReadableRetCode(sb, rc);
            sb.append(", ");
        }
        sb.setLength(sb.length() - 2);
        return sb.toString();
    }

    public abstract ApiCallRc executeApiCall();
}
