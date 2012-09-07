package com.orientechnologies.orient.server.hazelcast.sharding.hazelcast;

import com.hazelcast.config.FileSystemXmlConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import com.orientechnologies.common.hash.OMurmurHash3;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.ODHTNode;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.ODHTNodeLookup;
import com.orientechnologies.orient.server.hazelcast.sharding.distributed.OLocalDHTNode;

import java.io.FileNotFoundException;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Andrey Lomakin
 * @since 15.08.12
 */
public class ServerInstance implements MembershipListener, ODHTNodeLookup {
    public static final Map<String, ServerInstance> INSTANCES = new ConcurrentHashMap<String, ServerInstance>();

    private final ConcurrentHashMap<Long, Member> idMemberMap = new ConcurrentHashMap<Long, Member>();
    private volatile OLocalDHTNode localNode;
    private volatile HazelcastInstance hazelcastInstance;
    private final Timer timer = new Timer("DHT timer", true);
    private String configFile;

    public ServerInstance(String configFile) {
        this.configFile = configFile;
    }

    public void init() {
        try {
            hazelcastInstance = Hazelcast.newHazelcastInstance(new FileSystemXmlConfig(configFile));
        } catch (FileNotFoundException e) {
            throw new OConfigurationException("Error on creation Hazelcast instance");
        }

        localNode = new OLocalDHTNode(getNodeId(hazelcastInstance.getCluster().getLocalMember()));
        localNode.setNodeLookup(this);
        INSTANCES.put(hazelcastInstance.getCluster().getLocalMember().getUuid(), this);

        hazelcastInstance.getCluster().addMembershipListener(this);

        for (final Member member : hazelcastInstance.getCluster().getMembers()) {
            final long nodeId = getNodeId(member);
            if (nodeId != localNode.getNodeId())
                idMemberMap.put(nodeId, member);
        }

        if (idMemberMap.isEmpty())
            localNode.create();
        else {
            long oldestNodeId = getNodeId(hazelcastInstance.getCluster().getMembers().iterator().next());

            while (!localNode.join(oldestNodeId))
                oldestNodeId = getNodeId(hazelcastInstance.getCluster().getMembers().iterator().next());
        }

        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                localNode.stabilize();
                localNode.fixFingers();
            }
        }, 10000, 10000);
    }

    public ODHTNode findSuccessor(long key) {
        return findById(localNode.findSuccessor(key));
    }

    public void memberAdded(MembershipEvent membershipEvent) {
        final Member member = membershipEvent.getMember();
        final long nodeId = getNodeId(member);

        idMemberMap.put(nodeId, member);
        localNode.stabilize();
        localNode.fixFingers();
    }

    public void memberRemoved(MembershipEvent membershipEvent) {
        final Member member = membershipEvent.getMember();
        final long nodeId = getNodeId(member);

        idMemberMap.remove(nodeId);
        localNode.fixPredecessor();
        localNode.stabilize();
    }

    public ODHTNode findById(long id) {
        if (localNode.getNodeId() == id)
            return localNode;

        final Member member = idMemberMap.get(id);
        if (member == null)
            return null;

        return new OHazelcastDHTNodeProxy(id, member, hazelcastInstance);
    }

    protected long getNodeId(final Member iMember) {
        final String address = iMember.getInetSocketAddress().toString();
        final long nodeId = OMurmurHash3.murmurHash3_x64_64(address.getBytes(), 0);
        if (nodeId < 0)
            return -nodeId;

        return nodeId;
    }
}
