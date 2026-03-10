const adminPassword = process.env.MONGO_ROOT_PASSWORD;

if (!adminPassword) {
  throw new Error("MONGO_ROOT_PASSWORD not set");
}

try {
  const status = rs.status();
  if (status.ok) {
    print("Config replica set cfgRS already initialized.");
  }
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
    user: "admin",
    pwd: adminPassword,
    roles: [{ role: "root", db: "admin" }]
  });
  print("Admin user created on cfgRS.");
} catch (e) {
  const msg = e.toString();
  if (msg.includes("already exists") || msg.includes("DuplicateKey")) {
    print("Admin user already exists on cfgRS, skipping.");
  } else if (msg.includes("not authorized")) {
    print("Admin user likely already exists on cfgRS; skipping because auth is now enforced.");
  } else {
    throw e;
  }
}