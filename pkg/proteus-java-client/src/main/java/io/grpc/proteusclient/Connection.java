package io.grpc.proteusclient;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.proteusclient.QPUAPIGrpc.QPUAPIBlockingStub;
import io.grpc.proteusclient.Connection;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.Map;

public final class Connection {
  private final ManagedChannel channel;
  private final QPUAPIGrpc.QPUAPIBlockingStub stub;

  public Connection(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  public Connection(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    stub = QPUAPIGrpc.newBlockingStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().shutdownNow();
  }

  public QueryResp query(java.lang.String queryStr) {
    return stub.queryUnary(QueryReq.newBuilder().setQueryStr(queryStr).build());
  }
}