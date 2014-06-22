package com.cloudera.fraud.example.model;

public class ItemSaleEvent {
  private long buyingId;
  private long sellingId;
  private long itemID;
  private long itemValue;
  
  public ItemSaleEvent(long buyingUser, long sellingUser, long itemID,
      long itemValue) {
    super();
    this.buyingId = buyingUser;
    this.sellingId = sellingUser;
    this.itemID = itemID;
    this.itemValue = itemValue;
  }
  public long getBuyingId() {
    return buyingId;
  }
  public void setBuyingId(long buyingId) {
    this.buyingId = buyingId;
  }
  public long getSellingId() {
    return sellingId;
  }
  public void setSellingId(long sellingId) {
    this.sellingId = sellingId;
  }
  public long getItemID() {
    return itemID;
  }
  public void setItemID(long itemID) {
    this.itemID = itemID;
  }
  public long getItemValue() {
    return itemValue;
  }
  public void setItemValue(long itemValue) {
    this.itemValue = itemValue;
  }
	
	
	
}
