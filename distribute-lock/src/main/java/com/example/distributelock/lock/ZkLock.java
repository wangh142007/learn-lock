package com.example.distributelock.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

@Slf4j
public class ZkLock implements Watcher, AutoCloseable {

    private ZooKeeper zooKeeper;
    private String businessName;
    private String znode;

    public ZkLock(String connectString, String businessName) throws IOException {
        this.zooKeeper = new ZooKeeper(connectString, 30000, this);
        this.businessName = businessName;
    }

    public boolean getLock() throws KeeperException, InterruptedException {
        //创建业务 根节点
        Stat existsNode = zooKeeper.exists("/" + businessName, false);
        if (existsNode == null) {
            zooKeeper.create("/" + businessName, businessName.getBytes(),
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        //创建瞬时有序节点  /order/order_00000001
        znode = zooKeeper.create("/" + businessName + "/" + businessName + "_", businessName.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);

        znode = znode.substring(znode.lastIndexOf("/") + 1);
        //获取业务节点下 所有的子节点
        List<String> childrenNodes = zooKeeper.getChildren("/" + businessName, false);
        //子节点排序
        Collections.sort(childrenNodes);
        //获取序号最小的（第一个）子节点
        String firstNode = childrenNodes.get(0);

        //不是第一个子节点，则监听前一个节点

        /**
         * 当childrenNodes为 ：001 002 003
         * 当创建的节点不是第一节点时候  002！=001
         *
         * 让的前一个节点为lastNode = firstNode 获取到的第一个节点
         * 循环所有的节点
         * 判断当前节点是不是 为创建的节点  如果不是则将上一个节点复制为循环出来的node 如果是的话设置监听然后跳出循环
         *
         * 第一次循环
         *  001 不等于 002  lastNode = 001
         * 第二次循环
         *  002 等于 002 设置监听  然后跳出循环 让线程等待
         */
        if (!firstNode.equals(znode)) {
            String lastNode = firstNode;
            for (String node : childrenNodes) {
                if (!znode.equals(node)) {
                    lastNode = node;
                } else {
                    zooKeeper.exists("/" + businessName + "/" + lastNode, true);
                    break;
                }
            }
            synchronized (this) {
                wait();
            }
        }
        //否则获得锁
        return true;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
            synchronized (this) {
                notify();
            }
        }
    }


    @Override
    public void close() throws Exception {
        zooKeeper.delete("/" + businessName + "/" + znode, -1);
        zooKeeper.close();
        log.info("我释放了锁");
    }
}
