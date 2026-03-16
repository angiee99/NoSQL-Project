const bootstrapPassword = process.env.MONGO_BOOTSTRAP_PASSWORD;
if (!bootstrapPassword) throw new Error("MONGO_BOOTSTRAP_PASSWORD not set");

try {
  const status = rs.status();
  if (status.ok) print("Replica set rs1 already initialized.");
} catch (e) {
  print("Initializing replica set rs1...");
  rs.initiate({
    _id: "rs1",
    members: [
      { _id: 0, host: "mongo4:27017" },
      { _id: 1, host: "mongo5:27017" },
      { _id: 2, host: "mongo6:27017" }
    ]
  });
}

let isPrimary = false;
while (!isPrimary) {
  try {
    const hello = db.adminCommand({ hello: 1 });
    isPrimary = hello.isWritablePrimary || hello.ismaster;
    if (!isPrimary) {
      print("Waiting for rs1 primary election...");
      sleep(2000);
    }
  } catch (e) {
    print("Waiting for rs1 primary election...");
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
  print("bootstrapAdmin created on rs1.");
} catch (e) {
  const msg = e.toString();
  if (msg.includes("already exists") || msg.includes("DuplicateKey") || msg.includes("not authorized")) {
    print("bootstrapAdmin already exists on rs0, skipping.");
  } else {
    throw e;
  }
}