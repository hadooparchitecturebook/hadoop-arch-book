package com.hadooparchitecturebook.frauddetection.model;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.*;

/**
 * Created by ted.malaska on 1/16/15.
 */
public class UserProfile implements Cloneable{

  public String userId;
  public Long lastUpdatedTimeStamp;
  public HashMap<String, Long> spendByLast100VenderId = new HashMap<String, Long>();
  public Double historicAvgSingleDaySpend;
  public Double historicAvg90PercentSingleDaySpend;
  public Double todayMaxSpend;
  public Long todayNumOfPurchases;

  public UserProfile() {}

  public UserProfile(String string, Long lastUpdatedTimeStamp) throws JSONException {
    this(new JSONObject(string));
    this.lastUpdatedTimeStamp = lastUpdatedTimeStamp;
  }

  public UserProfile(JSONObject jsonObject)  throws JSONException {
    userId = jsonObject.getString("userId");
    historicAvgSingleDaySpend = jsonObject.getDouble("historicAvgSingleDaySpend");
    historicAvg90PercentSingleDaySpend = jsonObject.getDouble("historicAvg90PercentSingleDaySpend");
    todayMaxSpend = jsonObject.getDouble("todayMaxSpend");
    todayNumOfPurchases = jsonObject.getLong("todayNumOfPurchases");

    populateMapWithLong(jsonObject.getJSONObject("spendByLast100VenderId"), spendByLast100VenderId);
  }

  private void populateMapWithLong(JSONObject jsonObject, HashMap<String, Long> hashMap) throws JSONException {
    Iterator keys = jsonObject.keys();
    while (keys.hasNext()) {
      String key = keys.next().toString();
      hashMap.put(key, jsonObject.getLong(key));
    }
  }

  public JSONObject getJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("userId", userId);
    jsonObject.put("historicAvgSingleDaySpend", historicAvgSingleDaySpend);
    jsonObject.put("historicAvg90PercentSingleDaySpend", historicAvg90PercentSingleDaySpend);
    jsonObject.put("todayMaxSpend", todayMaxSpend);
    jsonObject.put("todayNumOfPurchases", todayNumOfPurchases);

    jsonObject.put("spendByLast100VenderId", new JSONObject(spendByLast100VenderId));

    return jsonObject;
  }

  public void updateWithUserEvent(UserEvent userEvent) {
    this.todayMaxSpend += userEvent.paymentAmount;
    this.todayNumOfPurchases += 1;

    spendByLast100VenderId.put(userEvent.vendorId, userEvent.timeStamp);

    if (spendByLast100VenderId.size() > 100) {
      String oldestVendorId = null;
      long oldestVendorTimeStamp = Long.MAX_VALUE;

      for (Map.Entry<String, Long> entry : spendByLast100VenderId.entrySet()) {
        if (entry.getValue().longValue() < oldestVendorTimeStamp) {
          oldestVendorTimeStamp = entry.getValue();
          oldestVendorId = entry.getKey();
        }
      }
      if (oldestVendorId != null) {
        spendByLast100VenderId.remove(oldestVendorId);
      }
    }
  }

  @Override
  public UserProfile clone() {
    try {
      return (UserProfile) super.clone();
    } catch (CloneNotSupportedException e) {
      e.printStackTrace();
      throw new RuntimeException();
    }
  }

}
