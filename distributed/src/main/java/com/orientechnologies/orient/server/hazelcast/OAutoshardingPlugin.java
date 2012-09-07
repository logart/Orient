package com.orientechnologies.orient.server.hazelcast;

import com.hazelcast.core.HazelcastInstance;
import com.orientechnologies.common.parser.OSystemVariableResolver;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.storage.OStorageEmbedded;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.hazelcast.sharding.OAutoshardedStorage;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.orientechnologies.orient.server.hazelcast.sharding.hazelcast.ServerInstance;

/**
 * @author <a href="mailto:enisher@exigenservices.com">Artem Orobets</a>
 * @since 8/27/12
 */
public class OAutoshardingPlugin extends OServerHandlerAbstract implements ODatabaseLifecycleListener {

    private boolean enabled = true;
    private ServerInstance serverInstance;

    @Override
    public String getName() {
        return "autosharding";
    }

    @Override
    public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
        oServer.setVariable("OAutoshardingPlugin", this);

        String configFile = "/hazelcast.xml";
        for (OServerParameterConfiguration param : iParams) {
            if (param.name.equalsIgnoreCase("enabled")) {
                if (!Boolean.parseBoolean(param.value)) {
                    enabled = false;
                    return;
                }
            } else if (param.name.equalsIgnoreCase("configuration.hazelcast")) {
                configFile = OSystemVariableResolver.resolveSystemVariables(param.value);
            }
        }

        serverInstance = new ServerInstance(configFile);
    }

    @Override
    public void startup() {
        if (!enabled)
            return;

        serverInstance.init();

        super.startup();
        Orient.instance().addDbLifecycleListener(this);
    }

    @Override
    public void onOpen(ODatabase iDatabase) {
        if (iDatabase instanceof ODatabaseComplex<?>) {
            iDatabase.replaceStorage(new OAutoshardedStorage(serverInstance, (OStorageEmbedded) iDatabase.getStorage()));
        }
    }

    @Override
    public void onClose(ODatabase iDatabase) {
    }
}
