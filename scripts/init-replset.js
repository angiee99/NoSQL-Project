const bootstrapPassword = process.env.MONGO_BOOTSTRAP_PASSWORD;
if (!bootstrapPassword) throw new Error("MONGO_BOOTSTRAP_PASSWORD not set");

const RS_NAME = process.env.MONGO_RS_NAME;
const LOCAL_HOST = process.env.MONGO_LOCAL_HOST;
const IS_CONFIGSVR = process.env.MONGO_IS_CONFIGSVR === "true";
const MEMBERS_JSON = process.env.MONGO_RS_MEMBERS_JSON;

if (!RS_NAME) throw new Error("MONGO_RS_NAME not set");
if (!LOCAL_HOST) throw new Error("MONGO_LOCAL_HOST not set");
if (!MEMBERS_JSON) throw new Error("MONGO_RS_MEMBERS_JSON not set");

// parse replica set members
let MEMBERS;
try {
  MEMBERS = JSON.parse(MEMBERS_JSON);
} catch (e) {
  throw new Error(`Failed to parse MONGO_RS_MEMBERS_JSON: ${e}`);
}

const WAIT_STEP_MS = 2000;
const MAX_WAIT_MS = 180000;
const OTHER_PRIMARY_GRACE_MS = 10000;

const adminDb = db.getSiblingDB("admin");

function helloSafe() {
  try {
    return db.adminCommand({ hello: 1 });
  } catch (e) {
    return null;
  }
}

function ensureReplicaSet() {
  const hello = helloSafe();

  // if replica set already exists, return
  if (hello && hello.setName === RS_NAME) {
    print(`Replica set ${RS_NAME} already initialized.`);
    return;
  }

  const config = {
    _id: RS_NAME,
    members: MEMBERS
  };

  if (IS_CONFIGSVR) {
    config.configsvr = true;
  }

  print(`Initializing replica set ${RS_NAME}...`);
  try {
    rs.initiate(config);
  } catch (e) {
    const msg = e.toString();
    if (
      msg.includes("already initialized") ||
      msg.includes("AlreadyInitialized")
    ) {
      print(`Replica set ${RS_NAME} was already initialized.`);
    } else {
      throw e;
    }
  }
}

function createBootstrapUser() {
  try {
    adminDb.createUser({
      user: "bootstrapAdmin",
      pwd: bootstrapPassword,
      roles: [
        { role: "userAdminAnyDatabase", db: "admin" },
        { role: "clusterAdmin", db: "admin" }
      ]
    });
    print(`bootstrapAdmin created on ${RS_NAME}.`);
    return true;
  } catch (e) {
    const msg = e.toString();
    if (
      msg.includes("already exists") ||
      msg.includes("DuplicateKey") ||
      msg.includes("not authorized") ||
      msg.includes("requires authentication")
    ) {
      print(`bootstrapAdmin already exists on ${RS_NAME}, skipping.`);
      return true;
    } else {
      throw e;
    }
  }
}

function ensureBootstrapUserWithRetry() {
  const deadline = Date.now() + MAX_WAIT_MS;
  let otherPrimarySince = null;

  while (Date.now() < deadline) {
    const hello = helloSafe();

    if (!hello || hello.setName !== RS_NAME || !hello.primary) {
      print(`Waiting for a PRIMARY in ${RS_NAME}...`);
      otherPrimarySince = null;
      sleep(WAIT_STEP_MS);
      continue;
    }

    const localWritable = !!(hello.isWritablePrimary || hello.ismaster);
    print(
      `Replica set ${RS_NAME} PRIMARY is ${hello.primary}. ` +
      `Local writable primary: ${localWritable}`
    );

    // if primary and writable - create user
    if (localWritable) {
      if (createBootstrapUser()) return;
    }

    // in case local node is primary, but isnt WritablePrimary yet, we wait a bit more
    if (hello.primary === LOCAL_HOST) {
      print(
        `Local node should become writable primary soon (${LOCAL_HOST}). Retrying...`
      );
      otherPrimarySince = null;
      sleep(WAIT_STEP_MS);
      continue;
    }

    // in case any other container is selected as primary, we wait a tiny more and then exit without creating any user.
    if (hello.primary !== LOCAL_HOST) {
      if (!otherPrimarySince) {
        otherPrimarySince = Date.now();
        print(`Another node is PRIMARY for ${RS_NAME} (${hello.primary}). Waiting briefly before exit...`);
      } else if (Date.now() - otherPrimarySince >= OTHER_PRIMARY_GRACE_MS) {
        print(`Another node remained PRIMARY for ${RS_NAME}. Exiting on ${LOCAL_HOST}.`);
        return;
      }
      sleep(WAIT_STEP_MS);
      continue;
    }
  }

  throw new Error(
    `Timed out waiting for bootstrapAdmin creation conditions on ${LOCAL_HOST} in ${RS_NAME}`
  );
}

ensureReplicaSet();
ensureBootstrapUserWithRetry();