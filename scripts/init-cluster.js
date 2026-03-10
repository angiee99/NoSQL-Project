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

ensureShard("rs0", "rs0/mongo1:27017");
ensureShard("rs1", "rs1/mongo4:27017");
ensureShard("rs2", "rs2/mongo7:27017");

print("Current shards:");
printjson(db.adminCommand({ listShards: 1 }));