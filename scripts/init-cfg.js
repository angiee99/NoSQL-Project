const bootstrapPassword = process.env.MONGO_BOOTSTRAP_PASSWORD;
if (!bootstrapPassword) throw new Error("MONGO_BOOTSTRAP_PASSWORD not set");

try {
  const status = rs.status();
  if (status.ok) print("Config replica set cfgRS already initialized.");
} catch (e) {
  print("Initializing config replica set cfgRS...");
  rs.initiate({
    _id: "cfgRS",
    configsvr: true,
    members: [
      { _id: 0, host: "cfg1:27019" },
      { _id: 1, host: "cfg2:27019" },
      { _id: 2, host: "cfg3:27019" }
    ]
  });
}

let isPrimary = false;
while (!isPrimary) {
  try {
    const hello = db.adminCommand({ hello: 1 });
    isPrimary = hello.isWritablePrimary || hello.ismaster;
    if (!isPrimary) {
      print("Waiting for cfgRS primary election...");
      sleep(2000);
    }
  } catch (e) {
    print("Waiting for cfgRS primary election...");
    sleep(2000);
  }
}

try {
  db.getSiblingDB("admin").createUser({
    user: "bootstrapAdmin",
    pwd: bootstrapPassword,
    roles: [
      { role: "userAdminAnyDatabase", db: "admin" },
      { role: "clusterAdmin", db: "admin" }
    ]
  });
  print("bootstrapAdmin created on cfgRS.");
} catch (e) {
  const msg = e.toString();
  if (msg.includes("already exists") || msg.includes("DuplicateKey") || msg.includes("not authorized")) {
    print("bootstrapAdmin already exists on cfgRS, skipping.");
  } else {
    throw e;
  }
}