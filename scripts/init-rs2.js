const adminPassword = process.env.MONGO_ROOT_PASSWORD;
if (!adminPassword) throw new Error("MONGO_ROOT_PASSWORD not set");

try {
  const status = rs.status();
  if (status.ok) print("Replica set rs2 already initialized.");
} catch (e) {
  print("Initializing replica set rs2...");
  rs.initiate({
    _id: "rs2",
    members: [
      { _id: 0, host: "mongo7:27017" },
      { _id: 1, host: "mongo8:27017" },
      { _id: 2, host: "mongo9:27017" }
    ]
  });
}

let isPrimary = false;
while (!isPrimary) {
  try {
    const hello = db.adminCommand({ hello: 1 });
    isPrimary = hello.isWritablePrimary || hello.ismaster;
    if (!isPrimary) {
      print("Waiting for rs2 primary election...");
      sleep(2000);
    }
  } catch (e) {
    print("Waiting for rs2 primary election...");
    sleep(2000);
  }
}

try {
  db.getSiblingDB("admin").createUser({
    user: "admin",
    pwd: adminPassword,
    roles: [{ role: "root", db: "admin" }]
  });
  print("Admin user created on rs2.");
} catch (e) {
  const msg = e.toString();
  if (msg.includes("already exists") || msg.includes("DuplicateKey") || msg.includes("not authorized")) {
    print("Admin user already exists on rs2, skipping.");
  } else {
    throw e;
  }
}