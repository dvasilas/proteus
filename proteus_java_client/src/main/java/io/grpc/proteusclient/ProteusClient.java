package io.grpc.proteusclient;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.proteusclient.QPUGrpc.QPUStub;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import java.util.Map;

public class ProteusClient {
  private final ManagedChannel channel;
  private final QPUStub asyncStub;

  public ProteusClient(String host, int port) {
    this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
  }

  public ProteusClient(ManagedChannelBuilder<?> channelBuilder) {
    channel = channelBuilder.build();
    asyncStub = QPUGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().shutdownNow();
  }

  public void query(QueryPredicate []predicates, Map<String, String> metadata, CountDownLatch finishLatch, final StreamObserver<ResponseStreamRecord> requestObserver, Boolean notify) {
    SnapshotTimePredicate clock;
    if (!notify) {
      clock = SnapshotTimePredicate.newBuilder()
        .setLbound(SnapshotTime.newBuilder().setType(SnapshotTime.SnapshotTimeType.LATEST).build())
        .setUbound(SnapshotTime.newBuilder().setType(SnapshotTime.SnapshotTimeType.LATEST).build())
        .build();
    } else {
      clock = SnapshotTimePredicate.newBuilder()
        .setLbound(SnapshotTime.newBuilder().setType(SnapshotTime.SnapshotTimeType.INF).build())
        .setUbound(SnapshotTime.newBuilder().setType(SnapshotTime.SnapshotTimeType.INF).build()).build();
    }

    QueryRequest.Builder builder = QueryRequest.newBuilder().setClock(clock);
    for (int i=0; i<predicates.length; i++) {
      QueryPredicate p = predicates[i];
      Attribute attribute = Attribute.newBuilder()
        .setAttrKey(p.getAttributeName())
        .setAttrType(p.getAttributeType()).build();

      Value lbound = Value.getDefaultInstance();
      Value ubound = Value.getDefaultInstance();
      switch (p.getAttributeType()) {
        case S3TAGINT:
          lbound = Value.newBuilder().setInt(p.getLBound().getIntValue()).build();
          ubound = Value.newBuilder().setInt(p.getUBound().getIntValue()).build();
          break;
        case S3TAGFLT:
          lbound = Value.newBuilder().setFlt(p.getLBound().getFloatValue()).build();
          ubound = Value.newBuilder().setFlt(p.getUBound().getFloatValue()).build();
          break;
        case S3TAGSTR:
          lbound = Value.newBuilder().setStr(p.getLBound().getStringValue()).build();
          ubound = Value.newBuilder().setStr(p.getUBound().getStringValue()).build();
          break;
        default:
          break;
      }
      AttributePredicate predicate = AttributePredicate.newBuilder()
        .setAttr(attribute)
        .setLbound(lbound)
        .setUbound(ubound)
        .build();
      builder.addPredicate(predicate);
    }

    if (metadata != null) {
      builder.putAllMetadata(metadata);
    }
    builder.putAllMetadata(metadata);
    QueryRequest qreq = builder.build();
    RequestStream req = RequestStream.newBuilder().setRequest(qreq).build();

    StreamObserver<RequestStream> toServer = asyncStub.query(
      new StreamObserver<ResponseStreamRecord>() {
      @Override
      public void onNext(ResponseStreamRecord record) {
        if (record.getType() != ResponseStreamRecord.StreamRecordType.HEARTBEAT) {
          requestObserver.onNext(record);
        }
      }
      @Override
      public void onError(Throwable t) { requestObserver.onError(t); }
      @Override
      public void onCompleted() { requestObserver.onCompleted(); }
    });
    toServer.onNext(req);
  }
}