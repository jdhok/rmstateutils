# rmstateutils
Tools to analyze Hadoop Yarn Resource Manager state store

Usage:

1. Checkout the repository to a directory in your system.
2. CD to the directory and run mvn package
3. After a successful build, rmstateutils-1.0-SNAPSHOT-jar-with-dependencies.jar should be present in the target directory.
4. Run the state copy class as follows
```
java -cp ~/etc/hadoop/conf:./rmstateutils-1.0-SNAPSHOT-jar-with-dependencies.jar org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateCopy  fs  zk
```
Hadoop conf dir and the 'jar with dependencies' must be present in the classpath. The same yarn-site.xml file can be used to configure both source and destination stores.
After the class name add the source and destination store nick names to the command line arguments.

There are four store copy options available

1. fs - file system state store
2. zk - ZooKeeper RM State store
3. mem - Memory RM state store
4. null - Null state store.

It is not possible to copy from fs to fs or from zk to zk. It has to be across two **different** state stores.
