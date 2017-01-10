package com.colobu.zkrecipe.leaderelection;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderLatchExample {
    private static final int CLIENT_QTY = 10;
    private static final String PATH = "/examples/leader";
    private static final Long SLEEP_TIME = 2000L;

    private static Logger logger = LoggerFactory.getLogger(LeaderLatchExample.class);

    public static void main(String[] args) throws Exception {

        List<CuratorFramework> clients = Lists.newArrayList();
        List<LeaderLatch> examples = Lists.newArrayList();
        TestingServer server = new TestingServer();
        try {
            // start LeaderLatch
            for (int i = 0; i < CLIENT_QTY; ++i) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                clients.add(client);
                client.start();

                LeaderLatch example = new LeaderLatch(client, PATH, "Client #" + i);
                examples.add(example);
                example.start();
            }

            // sleep and wait
            Thread.sleep(SLEEP_TIME);

            // find Leader
            LeaderLatch currentLeader = null;
            for (int i = 0; i < CLIENT_QTY; ++i) {
                LeaderLatch example = examples.get(i);
                if (example.hasLeadership()) {
                    currentLeader = example;
                }
            }
            logger.info("current leader is {}", currentLeader.getId());

            // release leader, remove it
            logger.info("release the leader {}", currentLeader.getId());
            currentLeader.close();
            examples.remove(currentLeader);

            // wait for new leader
            examples.get(0).await(2, TimeUnit.SECONDS);
            // check new leader
            logger.info("Client #0 maybe is elected as the leader or not although it want to be");
            logger.info("the new leader is {}", examples.get(0).getLeader().getId());

            logger.info("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("Shutting down...");
            for (LeaderLatch exampleClient : examples) {
                CloseableUtils.closeQuietly(exampleClient);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            CloseableUtils.closeQuietly(server);
        }
    }
}
