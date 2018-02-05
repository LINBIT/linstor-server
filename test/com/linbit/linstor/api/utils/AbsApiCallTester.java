package com.linbit.linstor.api.utils;

import java.util.ArrayList;
import java.util.List;

import com.linbit.linstor.api.ApiCallRc;
import com.linbit.linstor.api.utils.ApiCtrlAccessorTestImpl.StltConnectingAttempt;
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
        AccessContext accCtx,
        Peer peer,
        long objMask,
        long opMask,
        long... expectedRetCodes
    )
    {
        this.accCtx = accCtx;
        this.peer = peer;

        retCodes = new ArrayList<>();
        for (long expectedRetCode : expectedRetCodes)
        {
            retCodes.add(objMask | opMask | expectedRetCode);
        }

        expectedConnectingAttempts = new ArrayList<>();
    }

    public AbsApiCallTester accCtx(AccessContext accCtx)
    {
        this.accCtx = accCtx;
        return this;
    }

    public AbsApiCallTester peer(Peer peer)
    {
        this.peer = peer;
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
