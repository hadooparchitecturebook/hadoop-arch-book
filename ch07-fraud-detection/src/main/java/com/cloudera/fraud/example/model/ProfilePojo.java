package com.cloudera.fraud.example.model;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class ProfilePojo {
	//Fields that rarely change
  private String username;
  private int age;
  private long firstLogIn;
  private long lastLogIn;
  private String lastLogInIpAddress;
	
	//Field that change often 
  private long logInCount;
  
  private AtomicLong totalSells;
  private AtomicLong totalValueOfPastSells;
  private AtomicLong currentLogInSellsValue;
  
  private AtomicLong totalPurchases;
  private AtomicLong totalValueOfPastPurchases;
  private AtomicLong currentLogInPurchasesValue;
  
  private Set<String> last20LogOnIpAddresses;
  
  private long createdTimeStamp;
	
  public ProfilePojo(String username, int age, String ipAddress) {
    long time = System.currentTimeMillis();
    last20LogOnIpAddresses = new HashSet<String>();
    this.lastLogInIpAddress = ipAddress;
    this.firstLogIn = time;
    this.lastLogIn = time;
    this.logInCount = 0;
    this.createdTimeStamp = time;
  }
  
  public ProfilePojo(String username, int age, long firstLogIn, long lastLogIn,
      String lastLogInIpAddress,
      long logInCount, long totalSells, long totalValueOfPastSells,
      long currentLogInSellsValue, long totalPurchases,
      long totalValueOfPastPurchases, long currentLogInPurchasesValue,
      Set<String> last20LogOnIpAddresses) {
    super();
    this.username = username;
    this.age = age;
    this.firstLogIn = firstLogIn;
    this.lastLogIn = lastLogIn;
    this.lastLogInIpAddress = lastLogInIpAddress;
    this.logInCount = logInCount;
    this.totalValueOfPastSells.set(totalValueOfPastSells);
    this.currentLogInSellsValue.set(currentLogInSellsValue);
    this.totalValueOfPastPurchases.set(totalValueOfPastPurchases);
    this.currentLogInPurchasesValue.set(currentLogInPurchasesValue);
    this.last20LogOnIpAddresses = last20LogOnIpAddresses;
    this.totalSells.set(totalSells);
    this.totalPurchases.set(totalPurchases);
    this.createdTimeStamp = System.currentTimeMillis();
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public long getFirstLogIn() {
    return firstLogIn;
  }

  public void setFirstLogIn(long firstLogIn) {
    this.firstLogIn = firstLogIn;
  }

  public long getLastLogIn() {
    return lastLogIn;
  }

  public void setLastLogIn(long lastLogIn) {
    this.lastLogIn = lastLogIn;
  }

  public long getLogInCount() {
    return logInCount;
  }

  public void setLogInCount(long logInCount) {
    this.logInCount = logInCount;
  }

  public long getTotalValueOfPastSells() {
    return totalValueOfPastSells.get();
  }

  public void incrementTotalValueOfPastSells(long increment) {
    this.totalValueOfPastSells.addAndGet(increment);
  }

  public long getCurrentLogInSellsValue() {
    return currentLogInSellsValue.get();
  }

  public void incrementCurrentLogInSellsValue(long increment) {
    this.currentLogInSellsValue.addAndGet(increment);
  }

  public long getTotalValueOfPastPurchases() {
    return totalValueOfPastPurchases.get();
  }

  public void setTotalValueOfPastPurchases(long increment) {
    this.totalValueOfPastPurchases.addAndGet(increment);
  }

  public long getCurrentLogInPurchasesValue() {
    return currentLogInPurchasesValue.get();
  }

  public void incrementCurrentLogInPurchasesValue(long increment) {
    this.currentLogInPurchasesValue.addAndGet(increment);
  }

  public Set<String> getLast20LogOnIpAddresses() {
    return last20LogOnIpAddresses;
  }

  public void setLast20LogOnIpAddresses(Set<String> last20LogOnIpAddresses) {
    this.last20LogOnIpAddresses = last20LogOnIpAddresses;
  }

  public long getTotalSells() {
    return totalSells.get();
  }

  public void incrementTotalSells(long increment) {
    this.totalSells.addAndGet(increment);
  }

  public long getTotalPurchases() {
    return totalPurchases.get();
  }

  public void incrementTotalPurchases(long increment) {
    this.totalPurchases.addAndGet(increment);
  }
	
	public long getCreatedTimeStamp() {
	  return createdTimeStamp;
	}

  public String getLastLogInIpAddress() {
    return lastLogInIpAddress;
  }

  public void setLastLogInIpAddress(String lastLogInIpAddress) {
    this.lastLogInIpAddress = lastLogInIpAddress;
  }
	
}