const shardResult = db.adminCommand({ listShards: 1 });
const existingShards = (shardResult.shards || []).map(s => s._id);

if (existingShards.includes("rs0")) {
  print("Shard rs0 already added, skipping.");
} else {
  print("Adding shard rs0...");
  sh.addShard("rs0/mongo1:27017");
  print("Shard rs0 added.");
}

print("Current shards:");
printjson(db.adminCommand({ listShards: 1 }));