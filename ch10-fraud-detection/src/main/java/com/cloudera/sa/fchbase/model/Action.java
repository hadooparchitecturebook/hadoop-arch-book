package com.cloudera.sa.fchbase.model;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class Action {
  public UserEvent userEvent;
  public UserProfile userProfile;
  public boolean accept;
  public String alert;

  public Action(UserEvent userEvent, UserProfile userProfile, boolean accept, String alert) {
    this.userEvent = userEvent;
    this.userProfile = userProfile;
    this.accept = accept;
    this.alert = alert;
  }

  public Action(String string) throws JSONException {
    this(new JSONObject(string));
  }

  public Action(JSONObject jsonObject)  throws JSONException {
    userEvent = new UserEvent(jsonObject.getString("userEvent"));
    userProfile = new UserProfile(jsonObject.getString("userProfile"));
    accept = jsonObject.getBoolean("accept");
    alert = jsonObject.getString("alert");
  }

  public JSONObject getJSONObject() throws JSONException {
    JSONObject jsonObject = new JSONObject();
    jsonObject.put("userEvent", userEvent.getJSONObject());
    jsonObject.put("userProfile", userProfile.getJSONObject());
    jsonObject.put("accept", accept);
    jsonObject.put("alert", alert);
    return jsonObject;
  }
}
