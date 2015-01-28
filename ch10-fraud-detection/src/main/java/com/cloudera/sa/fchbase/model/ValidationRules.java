package com.cloudera.sa.fchbase.model;

import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.util.ajax.JSON;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class ValidationRules {

  public HashSet<String> bannedVanderIdSet;
  public Double thresholdInSpendDifferenceFromTodayFromPastMonthAverage;

  public static class Builder {
    public static ValidationRules buildValidationRules(NavigableMap<byte[], byte[]> familyMap) throws Exception {
      return new ValidationRules(Bytes.toString(familyMap.get(HBaseTableMetaModel.validationRulesRowKey)));
    }
  }

  public ValidationRules(HashSet<String> bannedVanderIdSet, Double thresholdInSpendDifferenceFromTodayFromPastMonthAverage) {
    this.bannedVanderIdSet = bannedVanderIdSet;
    this.thresholdInSpendDifferenceFromTodayFromPastMonthAverage = thresholdInSpendDifferenceFromTodayFromPastMonthAverage;
  }

  public ValidationRules(String jsonString) throws JSONException {
    this(new JSONObject(jsonString));
  }


  public ValidationRules(JSONObject jsonObject)  throws JSONException {
    JSONArray jsonArray = jsonObject.getJSONArray("bannedVanderIds");

    for (int i = 0; i < jsonArray.length(); i++) {
      bannedVanderIdSet.add(jsonArray.getString(i));
    }

    thresholdInSpendDifferenceFromTodayFromPastMonthAverage = jsonObject.getDouble("thresholdInSpendDifferenceFromTodayFromPastMonthAverage");

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
