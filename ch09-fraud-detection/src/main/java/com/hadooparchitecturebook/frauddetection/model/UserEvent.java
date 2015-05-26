package com.hadooparchitecturebook.frauddetection.model;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


/**
 * Created by ted.malaska on 1/18/15.
 */
public class UserEvent{
  public String userId;
  public Long timeStamp;
  public String ipAddress;
  public String countryCode;
  public String zipCode;
  public String itemCategory;
  public Double paymentAmount;
  public String vendorId;
  public boolean isCardPresent;

  public UserEvent(String userId, Long timeStamp, String ipAddress, String countryCode, String zipCode,
                   String itemCategory, Double paymentAmount, String vendorId, boolean isCardPresent) {
    this.userId = userId;
    this.timeStamp = timeStamp;
    this.ipAddress = ipAddress;
    this.countryCode = countryCode;
    this.zipCode = zipCode;
    this.itemCategory = itemCategory;
    this.paymentAmount = paymentAmount;
    this.vendorId = vendorId;
    this.isCardPresent = isCardPresent;
  }

  public UserEvent(String string) throws JSONException {
    this(new JSONObject(string));
  }

  public UserEvent(JSONObject jsonObject)  throws JSONException {
    userId = jsonObject.getString("userId");
    timeStamp = jsonObject.getLong("timeStamp");
    ipAddress = jsonObject.getString("ipAddress");
    countryCode = jsonObject.getString("countryCode");
    zipCode = jsonObject.getString("zipCode");
    itemCategory = jsonObject.getString("itemCategory");
    paymentAmount = jsonObject.getDouble("paymentAmount");
    vendorId = jsonObject.getString("vendorId");
    isCardPresent = jsonObject.getBoolean("isCardPresent");
  }

  public JSONObject getJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("userId", userId);
    jsonObject.put("timeStamp", timeStamp);
    jsonObject.put("ipAddress", ipAddress);
    jsonObject.put("countryCode", countryCode);
    jsonObject.put("zipCode", zipCode);
    jsonObject.put("itemCategory", itemCategory);
    jsonObject.put("paymentAmount", paymentAmount);
    jsonObject.put("vendorId", vendorId);
    jsonObject.put("isCardPresent", isCardPresent);
    return jsonObject;
  }
}
