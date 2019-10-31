package io.grpc.proteusclient;

public class QueryPredicate {
  private final String attributeName;
  private final Attribute.AttributeType attributeType;
  private final AttributeValue lbound;
  private final AttributeValue ubound;

  public QueryPredicate(String attributeName, Attribute.AttributeType attributeType, AttributeValue lbound,
      AttributeValue ubound) {
    this.attributeName = attributeName;
    this.attributeType = attributeType;
    this.lbound = lbound;
    this.ubound = ubound;
  }

  public String getAttributeName() {
    return this.attributeName;
  }

  public Attribute.AttributeType getAttributeType() {
    return this.attributeType;
  }

  public AttributeValue getLBound() {
    return this.lbound;
  }

  public AttributeValue getUBound() {
    return this.ubound;
  }
}