package com.hadooparchitecturebook.frauddetection.Utils;

import com.hadooparchitecturebook.frauddetection.model.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.codehaus.jettison.json.JSONException;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

/**
 * Created by ted.malaska on 1/18/15.
 */
public class UserProfileUtils {

  public static Logger LOG = Logger.getLogger(UserProfileUtils.class);

  public static UserProfile createUserProfile(NavigableMap<byte[], byte[]> familyMap) throws JSONException {

    long timeStamp = Bytes.toLong(familyMap.get(HBaseTableMetaModel.profileCacheTsColumn));

    String jsonString = Bytes.toString(familyMap.get(HBaseTableMetaModel.profileCacheJsonColumn));
    LOG.info("createUserProfile: " + jsonString);

    UserProfile userProfile = new UserProfile(jsonString, timeStamp);

    return userProfile;
  }

  public static Action reviewUserEvent(UserEvent userEvent, UserProfile userProfile, ValidationRules rules) {

    if (rules.bannedVanderIdSet.contains(userEvent.vendorId)) {

      List<String> alerts = new ArrayList<String>();

      Action action = new Action(userEvent, userProfile, false,
              "userEvent vendorId '" + userEvent.vendorId + "' is in banned venderId list.");

      return action;
    } else {
      Long timeStamp = userProfile.spendByLast100VenderId.get(userEvent.vendorId);
      if (timeStamp == null) {
        if (userEvent.paymentAmount - userProfile.historicAvgSingleDaySpend >
                userProfile.historicAvgSingleDaySpend * rules.thresholdInSpendDifferenceFromTodayFromPastMonthAverage) {
          List<String> alerts = new ArrayList<String>();

          Action action = new Action(userEvent, userProfile, false,
                  "userEvent vendorID '" + userEvent.vendorId + "' was not in last 100 list and threshold excised.");

          return action;
        }
      }
    }

    Action action = new Action(userEvent, userProfile, true,
            "accept");

    return action;
  }

}
