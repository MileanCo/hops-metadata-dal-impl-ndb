Hops Metadata DAL NDB Implementation
===

Hops Database abstraction layer for storing the hops metadata in MySQL Cluster

How to build
===

```
mvn clean install -DskipTests
```

If you get an error that LIBNDBPATH is not set (or not correct), go to the [Hops](https://github.com/hopshadoop/hops) folder, and then the /target/lib folder. Copy the complete path (find it with pwd), and add it to your .bashrc file:

```
export LIBNDBPATH=<your path here, e.g. /home/user/hops/hops/target/lib>
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$LIBNDBPATH
```

And reload bashrc with:

```
source ~/.bashrc
```

# Mac users

[Download MySQL Cluster](https://dev.mysql.com/downloads/cluster/)
Set the LDFLAGS to find protobuf 2.5 that you can install with "brew install protobuf@2.5" and the mysql-cluster library path.
Example:
```
export LDFLAGS="-L/usr/local/opt/protobuf@2.5/lib -L/usr/local/mysql-cluster-gpl-7.6.9-macos10.14-x86_64/lib"
```



Development Notes
===
Updates to the schema should be done in the schema/update-schema_XXX.sql corresponding to the version you are working on.

# License

Hops-Metadata-dal-impl-ndb is released under an [GPL 2.0 license](LICENSE.txt).
