package com.linbit.linstor.dbdrivers.interfaces;

import com.linbit.linstor.security.SecurityType;
import com.linbit.linstor.security.pojo.TypeEnforcementRulePojo;

/**
 * Database driver for {@link SecurityType}.
 *
 * @author Gabor Hernadi &lt;gabor.hernadi@linbit.com&gt;
 */
public interface SecTypeRulesDatabaseDriver extends GenericDatabaseDriver<TypeEnforcementRulePojo>
{
}
