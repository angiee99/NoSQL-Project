const DB_NAME = "projectdb";
const appDb = db.getSiblingDB(DB_NAME);
const configDb = db.getSiblingDB("config");

function assertOk(result, context) {
  if (!result || result.ok !== 1) {
    throw new Error(`${context} failed: ${JSON.stringify(result)}`);
  }
}

function ensureDatabaseSharding(dbName) {
  try {
    const result = sh.enableSharding(dbName);
    print(`Sharding enabled for database ${dbName}.`);
    if (result) printjson(result);
  } catch (e) {
    const msg = e.toString();
    if (
      msg.includes("already enabled") ||
      msg.includes("is already sharded")
    ) {
      print(`Sharding already enabled for database ${dbName}, skipping.`);
    } else {
      throw e;
    }
  }
}

function ensureCollection(name, validator) {
  const exists = appDb.getCollectionInfos({ name }).length > 0;

  if (!exists) {
    const result = appDb.createCollection(name, {
      validator,
      validationLevel: "strict",
      validationAction: "error"
    });
    print(`Collection ${DB_NAME}.${name} created.`);
    if (result) printjson(result);
    return;
  }

  const result = appDb.runCommand({
    collMod: name,
    validator,
    validationLevel: "strict",
    validationAction: "error"
  });
  assertOk(result, `collMod for ${DB_NAME}.${name}`);
  print(`Validator updated for ${DB_NAME}.${name}.`);
}

function isNamespaceSharded(ns) {
  return !!configDb.collections.findOne({ _id: ns });
}

function ensureSharded(collectionName, shardKey) {
  const ns = `${DB_NAME}.${collectionName}`;

  if (isNamespaceSharded(ns)) {
    print(`Collection ${ns} is already sharded, skipping.`);
    return;
  }

  const result = sh.shardCollection(ns, shardKey);
  print(`Collection ${ns} sharded with key ${JSON.stringify(shardKey)}.`);
  if (result) printjson(result);
}

function ensureIndex(collectionName, keySpec, options = {}) {
  const result = appDb.getCollection(collectionName).createIndex(keySpec, options);
  print(`Index ensured on ${DB_NAME}.${collectionName}: ${result}`);
}

// Validation schemas
const patientsValidator = {
  $jsonSchema: {
    bsonType: "object",
    required: [
      "patient_id",
      "first_name",
      "last_name",
      "dob",
      "age",
      "gender",
      "insurance_type",
      "registration_date"
    ],
    properties: {
      patient_id: {
        bsonType: "string"
      },
      first_name: {
        bsonType: "string"
      },
      last_name: {
        bsonType: "string"
      },
      dob: {
        bsonType: "date"
      },
      age: {
        bsonType: ["int", "long", "double", "decimal"],
        minimum: 0
      },
      gender: {
        bsonType: "string"
      },
      ethnicity: {
        bsonType: ["string", "null"]
      },
      insurance_type: {
        bsonType: "string"
      },
      marital_status: {
        bsonType: ["string", "null"]
      },
      contact: {
        bsonType: ["object", "null"],
        properties: {
          address: { bsonType: ["string", "null"] },
          city: { bsonType: ["string", "null"] },
          state: { bsonType: ["string", "null"] },
          zip: { bsonType: ["string", "null"] },
          phone: { bsonType: ["string", "null"] },
          email: { bsonType: ["string", "null"] }
        }
      },
      registration_date: {
        bsonType: "date"
      }
    }
  }
};

const encountersValidator = {
  $jsonSchema: {
    bsonType: "object",
    required: [
      "encounter_id",
      "patient_id",
      "provider_id",
      "visit_date",
      "visit_type",
      "department",
      "reason_for_visit",
      "diagnosis_code",
      "status",
      "readmitted_flag"
    ],
    properties: {
      encounter_id: {
        bsonType: "string"
      },
      patient_id: {
        bsonType: "string"
      },
      provider_id: {
        bsonType: "string"
      },
      visit_date: {
        bsonType: "date"
      },
      visit_type: {
        bsonType: "string"
      },
      department: {
        bsonType: "string"
      },
      reason_for_visit: {
        bsonType: "string"
      },
      diagnosis_code: {
        bsonType: "string"
      },
      admission: {
        bsonType: ["object", "null"],
        properties: {
          admission_type: { bsonType: ["string", "null"] },
          discharge_date: { bsonType: ["date", "null"] },
          length_of_stay: {
            bsonType: ["int", "long", "double", "decimal", "null"],
            minimum: 0
          }
        }
      },
      status: {
        bsonType: "string"
      },
      readmitted_flag: {
        bsonType: "bool"
      }
    }
  }
};

const claimsValidator = {
  $jsonSchema: {
    bsonType: "object",
    required: [
      "billing_id",
      "patient_id",
      "encounter_id",
      "insurance_provider",
      "payment_method",
      "claim",
      "amounts"
    ],
    properties: {
      billing_id: {
        bsonType: "string"
      },
      patient_id: {
        bsonType: "string"
      },
      encounter_id: {
        bsonType: "string"
      },
      insurance_provider: {
        bsonType: "string"
      },
      payment_method: {
        bsonType: "string"
      },
      claim: {
        bsonType: "object",
        required: ["claim_status"],
        properties: {
          claim_id: { bsonType: ["string", "null"] },
          claim_billing_date: { bsonType: ["date", "null"] },
          claim_status: { bsonType: "string" },
          denial_reason: { bsonType: ["string", "null"] }
        }
      },
      amounts: {
        bsonType: "object",
        required: ["billed_amount", "paid_amount"],
        properties: {
          billed_amount: {
            bsonType: ["decimal", "double", "int", "long"],
            minimum: 0
          },
          paid_amount: {
            bsonType: ["decimal", "double", "int", "long"],
            minimum: 0
          }
        }
      }
    }
  }
};


// Create or update collections if already exist
ensureDatabaseSharding(DB_NAME);

ensureCollection("patients", patientsValidator);
ensureCollection("encounters", encountersValidator);
ensureCollection("claims", claimsValidator);

// Shard collections by hashed patient_id for horizontal distribution.
ensureSharded("patients", { patient_id: "hashed" });
ensureSharded("encounters", { patient_id: "hashed" });
ensureSharded("claims", { patient_id: "hashed" });


// Secondary indexes for queries
// patients
ensureIndex("patients", { patient_id: 1 }, { name: "patient_id_1" });
ensureIndex("patients", { registration_date: 1 }, { name: "registration_date_1" });
ensureIndex(
  "patients",
  { insurance_type: 1, gender: 1 },
  { name: "insurance_type_1_gender_1" }
);
ensureIndex(
  "patients",
  { "contact.state": 1, "contact.city": 1 },
  { name: "contact_state_1_contact_city_1" }
);

// encounters
ensureIndex("encounters", { encounter_id: 1 }, { name: "encounter_id_1" });
ensureIndex(
  "encounters",
  { patient_id: 1, visit_date: -1 },
  { name: "patient_id_1_visit_date_-1" }
);
ensureIndex(
  "encounters",
  { department: 1, visit_type: 1 },
  { name: "department_1_visit_type_1" }
);
ensureIndex(
  "encounters",
  { diagnosis_code: 1 },
  { name: "diagnosis_code_1" }
);
ensureIndex(
  "encounters",
  { provider_id: 1 },
  { name: "provider_id_1" }
);
ensureIndex(
  "encounters",
  { status: 1, readmitted_flag: 1 },
  { name: "status_1_readmitted_flag_1" }
);

// claims
ensureIndex("claims", { billing_id: 1 }, { name: "billing_id_1" });
ensureIndex(
  "claims",
  { encounter_id: 1 },
  { name: "encounter_id_1" }
);
ensureIndex(
  "claims",
  { patient_id: 1, encounter_id: 1 },
  { name: "patient_id_1_encounter_id_1" }
);
ensureIndex(
  "claims",
  { patient_id: 1, "claim.claim_status": 1, "claim.claim_billing_date": -1 },
  { name: "patient_id_1_claim_status_1_claim_billing_date_-1" }
);
ensureIndex(
  "claims",
  { payment_method: 1, insurance_provider: 1 },
  { name: "payment_method_1_insurance_provider_1" }
);
ensureIndex(
  "claims",
  { "amounts.billed_amount": -1 },
  { name: "amounts_billed_amount_-1" }
);
ensureIndex(
  "claims",
  { "amounts.paid_amount": -1 },
  { name: "amounts_paid_amount_-1" }
);

print("init-projectdb.js finished successfully.");