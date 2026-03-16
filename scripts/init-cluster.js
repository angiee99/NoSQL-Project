function getAdminDb() {
  return db.getSiblingDB("admin");
}

function ensureShard(shardName, shardConnectionString) {
  const adminDb = getAdminDb();
  const result = adminDb.runCommand({ listShards: 1 });
  const existing = (result.shards || []).map(s => s._id);

  if (existing.includes(shardName)) {
    print(`Shard ${shardName} already added, skipping.`);
    return;
  }

  print(`Adding shard ${shardName} with ${shardConnectionString} ...`);
  const addResult = sh.addShard(shardConnectionString);

  if (addResult.ok !== 1) {
    throw new Error(`Failed to add shard ${shardName}: ${tojson(addResult)}`);
  }

  print(`Shard ${shardName} added.`);
}

function userExists(targetDb, username) {
  const usersInfo = targetDb.runCommand({
    usersInfo: { user: username, db: targetDb.getName() }
  });

  return usersInfo.ok === 1 && Array.isArray(usersInfo.users) && usersInfo.users.length > 0;
}

function ensureUser(username, pwd, roles, targetDbName = "admin") {
  const targetDb = db.getSiblingDB(targetDbName);

  if (userExists(targetDb, username)) {
    print(`User ${username} already exists in ${targetDbName}, skipping.`);
    return;
  }

  targetDb.createUser({
    user: username,
    pwd: pwd,
    roles: roles
  });

  print(`User ${username} created in ${targetDbName}.`);
}


ensureShard(
  "rs0",
  "rs0/mongo1:27017,mongo2:27017,mongo3:27017"
);

ensureShard(
  "rs1",
  "rs1/mongo4:27017,mongo5:27017,mongo6:27017"
);

ensureShard(
  "rs2",
  "rs2/mongo7:27017,mongo8:27017,mongo9:27017"
);

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
  process.env.MONGO_APP_PASSWORD,
  [
    { role: "readWrite", db: "projectdb" },
    { role: "dbAdmin", db: "projectdb" }
  ],
  "projectdb"
);

ensureUser(
  "appReader",
  process.env.MONGO_APP_PASSWORD,
  [
    { role: "read", db: "projectdb" }
  ],
  "projectdb"
);