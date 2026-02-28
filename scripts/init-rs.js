try {
  const status = rs.status();
  if (status.ok) {
    console.log("Replica set already initialized.");
  }
} catch (e) {
  console.log("Starting Replica Set initialization...");
  
  rs.initiate({
    _id: "rs0",
    members: [
      { _id: 0, host: "mongo1:27017" },
      { _id: 1, host: "mongo2:27017" },
      { _id: 2, host: "mongo3:27017" }
    ]
  });

  // Critical: Wait for this node to become PRIMARY
  // The localhost exception only works until the first user is created
  let isMaster = false;
  while (!isMaster) {
    const hello = db.runCommand({ hello: 1 });
    isMaster = hello.isWritablePrimary || hello.ismaster;
    if (!isMaster) {
      console.log("Waiting for node to become Primary...");
      sleep(2000);
    }
  }

  console.log("Node is Primary. Creating Admin user...");
  db.getSiblingDB("admin").createUser({
    user: "admin",
    pwd: "password123", 
    roles: [{ role: "root", db: "admin" }]
  });
  console.log("Setup complete!");
}