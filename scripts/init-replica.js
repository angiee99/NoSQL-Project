// init-replica.js
const MAX_RETRIES = 60;
let retries = 0;

// Initiate the replica set
try {
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
    ]
  });
  print("Replica set initiation command sent.");
} catch (e) {
  print("rs.initiate() may have already run: ", e);
}

// Wait for PRIMARY
let status = rs.status();
while (status.myState !== 1 && retries < MAX_RETRIES) {
  print("Waiting for PRIMARY...");
  retries++;
  sleep(1000);
  status = rs.status();
}

if (status.myState !== 1) {
  print("Error: Primary not elected after wait. Exiting.");
  quit(1);
}

print("Primary elected successfully.");

// Create admin user (only now)
let dbAdmin = db.getSiblingDB("admin");
if (!dbAdmin.getUser("admin")) {
  print("Creating admin user...");
  dbAdmin.createUser({
    user: "admin",
    pwd: "adminpassword",
    roles: [{ role: "root", db: "admin" }]
  });
  print("Admin user created successfully.");
} else {
  print("Admin user already exists.");
}