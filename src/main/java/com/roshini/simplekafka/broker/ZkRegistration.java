package com.roshini.simplekafka.broker;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class ZkRegistration {

    private final ZooKeeper zooKeeper;

    public ZkRegistration(String zkAddress) throws IOException {
        this.zooKeeper = new ZooKeeper(zkAddress, 3000, event -> {
            System.out.println("ZooKeeper event: " + event);
        });
    }

    public void registerBroker(int brokerId, String host, int port) throws Exception {
        String brokersPath = "/brokers";

        if (zooKeeper.exists(brokersPath, false) == null) {
            zooKeeper.create(brokersPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        String brokerPath = brokersPath + "/" + brokerId;
        String data = host + ":" + port;

        zooKeeper.create(brokerPath, data.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        System.out.println("Registered broker " + brokerId + " at " + brokerPath);
    }

}