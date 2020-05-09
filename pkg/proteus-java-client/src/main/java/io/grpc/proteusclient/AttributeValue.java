package io.grpc.proteusclient;

public class AttributeValue {
  private java.lang.Object val;

  public AttributeValue(long value) {
    this.val = value;
  }

  public AttributeValue(double value) {
    this.val = value;
  }

  public AttributeValue(String value) {
    this.val = value;
  }

  public long getIntValue() {
    if (val instanceof java.lang.Long) {
      return (java.lang.Long) val;
    }
    return 0L;
  }

  public double getFloatValue() {
    if (val instanceof java.lang.Double) {
      return (java.lang.Double) val;
    }
    return 0D;
  }

  public String getStringValue() {
    if (val instanceof java.lang.String) {
      return (java.lang.String) val;
    }
    return "";
  }
}
