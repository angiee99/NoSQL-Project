const adminPassword = process.env.MONGO_ROOT_PASSWORD;
if (!adminPassword) throw new Error("MONGO_ROOT_PASSWORD not set");

try {
  const status = rs.status();
  if (status.ok) print("Replica set rs0 already initialized.");
} catch (e) {
  print("Initializing replica set rs0...");
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
    ]
  });
}

let isPrimary = false;
while (!isPrimary) {
  try {
    const hello = db.adminCommand({ hello: 1 });
    isPrimary = hello.isWritablePrimary || hello.ismaster;
    if (!isPrimary) {
      print("Waiting for rs0 primary election...");
      sleep(2000);
    }
  } catch (e) {
    print("Waiting for rs0 primary election...");
    sleep(2000);
  }
}

try {
  db.getSiblingDB("admin").createUser({
    user: "admin",
    pwd: adminPassword,
    roles: [{ role: "root", db: "admin" }]
  });
  print("Admin user created on rs0.");
} catch (e) {
  const msg = e.toString();
  if (msg.includes("already exists") || msg.includes("DuplicateKey") || msg.includes("not authorized")) {
    print("Admin user already exists on rs0, skipping.");
  } else {
    throw e;
  }
}