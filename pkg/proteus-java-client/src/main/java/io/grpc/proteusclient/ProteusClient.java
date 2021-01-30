package io.grpc.proteusclient;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.proteusclient.QPUAPIGrpc.QPUAPIBlockingStub;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public final class ProteusClient {
  private static int maxPoolSize;
  private int initialPoolSize;
  private int currentPoolSize;
  private String host;
  private int port;

  private BlockingQueue<Connection> pool;

  public ProteusClient(int initialPoolSize, int maxPoolSize, String host, int port) {
    this.maxPoolSize = maxPoolSize > 0 ? maxPoolSize : 10;
    this.initialPoolSize = initialPoolSize;
    this.host = host;
    this.port = port;

    this.pool = new LinkedBlockingQueue<Connection>(maxPoolSize);

    for (int i = 0; i < initialPoolSize; i++) {
      openAndPoolConnection();
    }
  }

  private synchronized void openAndPoolConnection()  {
    if (currentPoolSize == maxPoolSize) {
      return;
    }

    pool.offer(new Connection(host, port));
    currentPoolSize++;
  }

  private Connection getConnection() throws InterruptedException {
    if (pool.peek()==null && currentPoolSize < maxPoolSize) {
      openAndPoolConnection();
    }

  return pool.take();
}

  public void releaseConnection(Connection connection) {
    pool.offer(connection);
  }

  public void shutdown() throws InterruptedException {
    while (pool.peek()!=null) {
      Connection connection = pool.poll();
      connection.shutdown();
    }
  }

  public QueryResp1 query(java.lang.String queryStr) throws InterruptedException {
    Connection connection = getConnection();
    QueryResp1 resp = connection.query(queryStr);
    releaseConnection(connection);
    return resp;
  }
}