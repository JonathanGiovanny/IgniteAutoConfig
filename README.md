# IgniteAutoConfig
Notations to configure automatically the Apache Ignite sesttings (for now tested on standalone IMDG cache).
The configurations are being set for each table, so it is like mapping on hibernate + the Ignite notations.

The notations used are:
@IgniteTable
@IgniteId
@IgniteColumn

It should be loaded with
```java
IgniteAutoConfig.loadConfiguration(Student.class);
```

Where Student is the class with the notations and the one that we want that Apache Ignite consider loading in the cache.
