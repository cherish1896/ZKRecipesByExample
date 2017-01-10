package com.colobu.zkrecipe.leaderelection;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class ExampleClient extends LeaderSelectorListenerAdapter implements Closeable {

    private static Logger logger = LoggerFactory.getLogger(ExampleClient.class);
    private final String name;
    private final LeaderSelector leaderSelector;
    private final AtomicInteger leaderCount = new AtomicInteger();

    public ExampleClient(CuratorFramework client, String path, String name) {
        this.name = name;
        leaderSelector = new LeaderSelector(client, path, this);
        leaderSelector.autoRequeue();
    }

    public void start() throws IOException {
        leaderSelector.start();
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    @Override
    public void takeLeadership(CuratorFramework client) throws Exception {
        final int waitSeconds = (int) (5 * Math.random()) + 1;
        logger.info("{} is now the leader. Waiting {} seconds...", this.name, waitSeconds);
        logger.info("{} has been leader {} time(s) before.", this.name, leaderCount.getAndIncrement());
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(waitSeconds));
        } catch (InterruptedException e) {
            logger.error(name + " was interrupted.", e);
            Thread.currentThread().interrupt();
        } finally {
            logger.info(name + " relinquishing leadership now.\n");
        }
    }
}