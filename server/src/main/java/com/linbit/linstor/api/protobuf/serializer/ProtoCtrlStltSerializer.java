package com.linbit.linstor.api.protobuf.serializer;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.linbit.linstor.annotation.ApiContext;
import com.linbit.linstor.api.interfaces.serializer.CtrlStltSerializer;
import com.linbit.linstor.core.CtrlSecurityObjects;
import com.linbit.linstor.core.LinStor;
import com.linbit.linstor.logging.ErrorReporter;
import com.linbit.linstor.propscon.Props;
import com.linbit.linstor.security.AccessContext;

@Singleton
public class ProtoCtrlStltSerializer extends ProtoCommonSerializer
    implements CtrlStltSerializer
{
    private final CtrlSecurityObjects secObjs;
    private final Props ctrlConf;

    @Inject
    public ProtoCtrlStltSerializer(
        ErrorReporter errReporter,
        @ApiContext AccessContext serializerCtx,
        CtrlSecurityObjects secObjsRef,
        @Named(LinStor.SATELLITE_PROPS) Props ctrlConfRef)
    {
        super(errReporter, serializerCtx);
        secObjs = secObjsRef;
        ctrlConf = ctrlConfRef;
    }

    @Override
    public CtrlStltSerializerBuilder builder()
    {
        return builder(null);
    }

    @Override
    public CtrlStltSerializerBuilder builder(String apiCall)
    {
        return builder(apiCall, 1);
    }

    @Override
    public CtrlStltSerializerBuilder builder(String apiCall, Integer msgId)
    {
        return new ProtoCtrlStltSerializerBuilder(
            errorReporter,
            serializerCtx,
            secObjs,
            ctrlConf,
            apiCall,
            msgId
        );
    }
}
