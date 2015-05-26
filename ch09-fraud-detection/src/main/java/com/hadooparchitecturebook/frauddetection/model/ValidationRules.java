package com.hadooparchitecturebook.frauddetection.model;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class ValidationRules {

  static Logger LOG = Logger.getLogger(ValidationRules.class);

  public HashSet<String> bannedVanderIdSet = new HashSet<String>();
  public Double thresholdInSpendDifferenceFromTodayFromPastMonthAverage = -1.0;

  public static class Builder {
    public static ValidationRules buildValidationRules(NavigableMap<byte[], byte[]> familyMap) throws Exception {

      if (familyMap != null) {
        byte[] bytes = familyMap.get(HBaseTableMetaModel.validationRulesRowKey);

        return new ValidationRules(Bytes.toString(bytes));
      } else {
        LOG.warn("No Validation Rules Found in HBase");
        return new ValidationRules();
      }
    }
  }

  public ValidationRules() {

  }

  public ValidationRules(HashSet<String> bannedVanderIdSet, Double thresholdInSpendDifferenceFromTodayFromPastMonthAverage) {
    this.bannedVanderIdSet = bannedVanderIdSet;
    this.thresholdInSpendDifferenceFromTodayFromPastMonthAverage = thresholdInSpendDifferenceFromTodayFromPastMonthAverage;
  }

  public ValidationRules(String jsonString) throws JSONException {
    this(new JSONObject(jsonString));
  }


  public ValidationRules(JSONObject jsonObject)  throws JSONException {
    if (jsonObject != null) {
      JSONArray jsonArray = jsonObject.getJSONArray("bannedVanderIds");

      for (int i = 0; i < jsonArray.length(); i++) {
        String bannedId = jsonArray.getString(i);
        LOG.info(" - Adding bannded venderId:" + bannedId);
        bannedVanderIdSet.add(bannedId);
      }

      thresholdInSpendDifferenceFromTodayFromPastMonthAverage = jsonObject.getDouble("thresholdInSpendDifferenceFromTodayFromPastMonthAverage");
    } else {
      LOG.warn("No Validation Rules Found in HBase");
    }
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
    jsonObject.put("bannedVanderIdSet", bannedVanderIdSet);
    jsonObject.put("thresholdInSpendDifferenceFromTodayFromPastMonthAverage", thresholdInSpendDifferenceFromTodayFromPastMonthAverage);

    return jsonObject;
  }

}
