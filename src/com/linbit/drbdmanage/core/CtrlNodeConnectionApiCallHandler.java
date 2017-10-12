package com.linbit.drbdmanage.core;

import static com.linbit.drbdmanage.ApiCallRcConstants.RC_NODE_CRT_FAIL_SQL_ROLLBACK;

import java.sql.SQLException;
import java.util.Map;

import com.linbit.TransactionMgr;
import com.linbit.drbdmanage.ApiCallRc;
import com.linbit.drbdmanage.ApiCallRcImpl;
import com.linbit.drbdmanage.ApiCallRcImpl.ApiCallRcEntry;
import com.linbit.drbdmanage.api.ApiConsts;
import com.linbit.drbdmanage.netcom.Peer;
import com.linbit.drbdmanage.security.AccessContext;

class CtrlNodeConnectionApiCallHandler
{
    private Controller controller;

    CtrlNodeConnectionApiCallHandler(Controller controllerRef)
    {
        controller = controllerRef;
    }

    public ApiCallRc createNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1Str,
        String nodeName2Str,
        Map<String, String> nodeConnPropsMap
    )
    {
        ApiCallRcImpl apiCallRc = new ApiCallRcImpl();
        TransactionMgr transMgr = null;



        if (transMgr != null)
        {
            if (transMgr.isDirty())
            {
                try
                {
                    transMgr.rollback();
                }
                catch (SQLException sqlExc)
                {
                    String errorMessage = String.format(
                        "A database error occured while trying to rollback the creation of a connection " +
                            "between the nodes '%s' and '%s'.",
                        nodeName1Str,
                        nodeName2Str
                    );
                    controller.getErrorReporter().reportError(
                        sqlExc,
                        accCtx,
                        client,
                        errorMessage
                    );

                    ApiCallRcEntry entry = new ApiCallRcEntry();
                    entry.setReturnCodeBit(RC_NODE_CRT_FAIL_SQL_ROLLBACK);
                    entry.setMessageFormat(errorMessage);
                    entry.setCauseFormat(sqlExc.getMessage());
                    entry.putObjRef(ApiConsts.KEY_1ST_NODE, nodeName1Str);
                    entry.putObjRef(ApiConsts.KEY_2ND_NODE, nodeName2Str);

                    apiCallRc.addEntry(entry);
                }
            }
            controller.dbConnPool.returnConnection(transMgr.dbCon);
        }
        return apiCallRc;
    }

    public ApiCallRc deleteNodeConnection(
        AccessContext accCtx,
        Peer client,
        String nodeName1,
        String nodeName2
    )
    {
        // TODO Auto-generated method stub
        return null;
    }

}
