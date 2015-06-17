package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.AMRMTokenSecretManagerState;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationAttemptStateData;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.ApplicationStateData;
import org.apache.log4j.PropertyConfigurator;

import com.google.common.base.Strings;

/**
 * Copy the application state maintained by Hadoop Yarn's resource manager from one state store to another.
 * This is useful in preserving state while switching store implementations in the resource manager.
 *
 * This can be also used to backup state in different stores during upgrades or restarts.
 * For example if ZK state store is being used, we can copy it to FS state store, and use in case ZK state store
 * becomes unavailable.
 *
 */
public class RMStateCopy {
  private static final Log LOG = LogFactory.getLog(RMStateCopy.class);

  private static final Map<String, Class> stateStoreClasses = new HashMap<>();

  static {
    stateStoreClasses.put("fs", FileSystemRMStateStore.class);
    stateStoreClasses.put("zk", ZKRMStateStore.class);
    stateStoreClasses.put("null", NullRMStateStore.class);
    stateStoreClasses.put("mem", MemoryRMStateStore.class);
  }

  static void configureLogging() throws IOException {
    Properties props = new Properties();
    props.load(RMStateCopy.class.getResourceAsStream("/log4j.properties"));
    PropertyConfigurator.configure(props);
  }

  static RMStateStore getStateStore(String storeNickName, YarnConfiguration yarnConf) throws Exception {
    Configuration conf = new Configuration(yarnConf);
    conf.set(YarnConfiguration.RM_STORE, stateStoreClasses.get(storeNickName).getName());

    RMStateStore store = RMStateStoreFactory.getStore(conf);
    store.init(conf);
    store.start();
    return store;
  }

  static void die(String msg) {
    System.err.println(msg);
    System.exit(1);
  }

  static void checkNickName(String nick) {
    if (Strings.isNullOrEmpty(nick) || !stateStoreClasses.containsKey(nick)) {
      die("Invalid store nick name: '" + nick + "' Allowed values are " + stateStoreClasses.keySet().toString());
    }
  }


  /**
   * First argument is the nick name for the source state store.
   * Second argument is the nick name for the destination state store.
   * Source and destination stores need to be different.
   *
   * Yarn configuration (yarn-site.xml) must be available on the classpath. RMStateStore specific configuration
   * can be set in the yarn-site.xml
   *
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      die("Usage: java RMStateCopy <source store nick> <dest store nick>");
    }

    String srcNick = args[0];
    String destNick = args[1];

    checkNickName(srcNick);
    checkNickName(destNick);

    if (srcNick.equals(destNick)) {
      die("Source and destination stores are same: " + srcNick);
    }

    try {
      configureLogging();
      copyStateStores(srcNick, destNick);
    } catch (Exception e) {
      LOG.error("Error copying stores from " + srcNick + " to " + destNick, e);
      System.exit(1);
    }
  }


  static void copyStateStores(String srcNick, String destNick) throws Exception {
    YarnConfiguration yarnConf = new YarnConfiguration();

    RMStateStore src = getStateStore(srcNick, yarnConf);
    LOG.info("Initialized source store " + src);
    RMStateStore dest = getStateStore(destNick, yarnConf);
    LOG.info("Initialized destination store " + dest);

    dest.storeVersion();
    LOG.info("Stored version info in destination store");

    RMStateStore.RMState state = src.loadState();
    LOG.info("Loaded state from source store");

    AMRMTokenSecretManagerState tokenState = state.getAMRMTokenSecretManagerState();
    copyAMRMTokenState(tokenState, dest);
    LOG.info("Copied AM-RM token secret manager state to destination store");

    Map<ApplicationId, RMStateStore.ApplicationState> appState = state.getApplicationState();
    copyAppState(appState, dest);
    LOG.info("Copied application state to destination store");

    RMStateStore.RMDTSecretManagerState dtState = state.getRMDTSecretManagerState();
    copyRMDTState(dtState, dest);
    LOG.info("Copied RM delegation token manager state to destination store");

    closeStore(src);
    LOG.info("Closed source store");
    closeStore(dest);
    LOG.info("Closed destination store");
  }

  //TODO Check if we need to copy app state in parallel to speed up copy process
  static void copyAppState(Map<ApplicationId, RMStateStore.ApplicationState> appStates, RMStateStore dest)
    throws Exception {
    LOG.info("Copying " + appStates.size() + " applications");
    for (Map.Entry<ApplicationId, RMStateStore.ApplicationState> entry : appStates.entrySet()) {
      ApplicationId appID = entry.getKey();
      RMStateStore.ApplicationState appState = entry.getValue();

      ApplicationStateData stateData = ApplicationStateData.newInstance(appState);
      dest.storeApplicationStateInternal(appID, stateData);

      LOG.info("Copying application " + appID);

      for (int i = 0; i < appState.getAttemptCount(); i++) {
        ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(appID, i + 1);
        LOG.info("Copying attempt " + appAttemptId);
        RMStateStore.ApplicationAttemptState attemptState = appState.getAttempt(appAttemptId);
        dest.storeApplicationAttemptStateInternal(appAttemptId,
          ApplicationAttemptStateData.newInstance(attemptState));
      }
    }
  }

  public static void copyAMRMTokenState(AMRMTokenSecretManagerState tokenState, RMStateStore dest) throws Exception {
    dest.storeOrUpdateAMRMTokenSecretManagerState(tokenState, false);
  }

  static void copyRMDTState(RMStateStore.RMDTSecretManagerState dtState, RMStateStore dest) {
    // TODO How to save this sequence number?
    int seqNo = dtState.getDTSequenceNumber();

    Set<DelegationKey> masterKeyState = dtState.getMasterKeyState();
    Map<RMDelegationTokenIdentifier, Long> tokenState = dtState.getTokenState();

    LOG.info("Copying " + tokenState.size() + " RM delegation tokens");

    for (Map.Entry<RMDelegationTokenIdentifier, Long> tokenEntry : tokenState.entrySet()) {
      RMDelegationTokenIdentifier rmdtTokenId = tokenEntry.getKey();
      Long rmdtTokenRenewDate = tokenEntry.getValue();
      int tokenSeqNo = rmdtTokenId.getSequenceNumber();
      dest.storeRMDelegationTokenAndSequenceNumber(rmdtTokenId, rmdtTokenRenewDate, tokenSeqNo);
    }

    for (DelegationKey delegationKey : masterKeyState) {
      dest.storeRMDTMasterKey(delegationKey);
    }
  }

  static void closeStore(RMStateStore store) throws Exception {
    store.stop();
  }

}
