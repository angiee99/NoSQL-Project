function ensureShard(shardName, shardConnectionString) {
  const result = db.adminCommand({ listShards: 1 });
  const existing = (result.shards || []).map(s => s._id);

  if (existing.includes(shardName)) {
    print(`Shard ${shardName} already added, skipping.`);
    return;
  }

  print(`Adding shard ${shardName}...`);
  sh.addShard(shardConnectionString);
  print(`Shard ${shardName} added.`);
}

function ensureUser(username, pwd, roles, targetDbName = "admin") {
  const targetDb = db.getSiblingDB(targetDbName);
  try {
    targetDb.createUser({
      user: username,
      pwd,
      roles
    });
    print(`User ${username} created in ${targetDbName}.`);
  } catch (e) {
    const msg = e.toString();
    if (msg.includes("already exists") || msg.includes("DuplicateKey")) {
      print(`User ${username} already exists in ${targetDbName}, skipping.`);
    } else {
      throw e;
    }
  }
}

ensureShard("rs0", "rs0/mongo1:27017,mongo2:27017,mongo3:27017");
ensureShard("rs1", "rs1/mongo4:27017,mongo5:27017,mongo6:27017");
ensureShard("rs2", "rs2/mongo7:27017,mongo8:27017,mongo9:27017");

ensureUser(
  "clusterAdminUser",
  process.env.MONGO_CLUSTER_ADMIN_PASSWORD,
  [
    { role: "clusterAdmin", db: "admin" },
    { role: "userAdminAnyDatabase", db: "admin" },
    { role: "readWriteAnyDatabase", db: "admin" },
    { role: "dbAdminAnyDatabase", db: "admin" }
  ],
  "admin"
);

ensureUser(
  "appWriter",
  process.env.MONGO_APP_WRITE_PASSWORD,
  [
    { role: "readWrite", db: "projectdb" },
    { role: "dbAdmin", db: "projectdb" }
  ],
  "projectdb"
);

ensureUser(
  "appReader",
  process.env.MONGO_APP_READ_PASSWORD,
  [
    { role: "read", db: "projectdb" }
  ],
  "projectdb"
);

print("Current shards:");
printjson(db.adminCommand({ listShards: 1 }));