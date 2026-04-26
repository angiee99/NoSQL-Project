## Aggregation 
1. Claim status averages amounts
Get the average billed and paid amounts for both all claim statuses (Paid/Denied)

```
db.claims.aggregate(
  [
    {
      $match: {
      "claim.claim_status": { $ne: null }
      }
    },
    {
      $group: {
        _id: '$claim.claim_status',
        avg_billed: {
          $avg: '$amounts.billed_amount'
        },
        avg_paid: {
          $avg: '$amounts.paid_amount'
        },
        claim_count: { $sum: 1 }
      }
    },
    {
      $project: {
        _id: 0,
        claim_status: "$_id",
        claim_count: 1,
        avg_billed: { $round: ["$avg_billed", 2] },
        avg_paid: { $round: ["$avg_paid", 2] }
      }
    },
    { $sort: { claim_count: -1 } }
  ]
);
```

2. Top insurance by state
The most popular insurance type per state with the count of patients with that insurance sorted by state

```
db.patients.aggregate([
  {
    $match: {
      "contact.state": { $ne: null },
      insurance_type: { $ne: null }
    }
  },
  {
    $group: {
      _id: {
        state: "$contact.state",
        insurance_type: "$insurance_type"
      },
      patient_count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.state": 1,
      patient_count: -1
    }
  },
  {
    $group: {
      _id: "$_id.state",
      most_popular_insurance_type: { $first: "$_id.insurance_type" },
      patients_with_that_insurance: { $first: "$patient_count" }
    }
  },
  {
    $project: {
      _id: 0,
      state: "$_id",
      most_popular_insurance_type: 1,
      patients_with_that_insurance: 1
    }
  },
  {
    $sort: { state: 1 }
  }
])
```

3. Emergency stay and readmission
Analyze emergency admissions by length of stay and count how many of them were later marked as readmitted.

```
db.encounters.aggregate([
  {
    $match: {
      "admission.admission_type": "Emergency",
      "admission.length_of_stay": { $ne: null }
    }
  },
  {
    $group: {
      _id: "$admission.length_of_stay",
      emergency_encounters: { $sum: 1 },
      readmitted_cases: {
        $sum: {
          $cond: [{ $eq: ["$readmitted_flag", true] }, 1, 0]
        }
      }
    }
  },
  {
    $addFields: {
      readmission_rate_pct: {
        $round: [
          {
            $multiply: [
              { $divide: ["$readmitted_cases", "$emergency_encounters"] },
              100
            ]
          },
          2
        ]
      }
    }
  },
  {
    $project: {
      _id: 0,
      length_of_stay_days: "$_id",
      emergency_encounters: 1,
      readmitted_cases: 1,
      readmission_rate_pct: 1
    }
  },
  {
    $sort: { length_of_stay_days: 1 }
  }
]);
```

4. Denial reason financial analysis
Show which denial reasons are most common and how much billed money is associated with each denial reason.

```
db.claims.aggregate([
  {
    $match: {
      "claim.claim_status": "Denied",
      "claim.denial_reason": { $nin: [null, ""] }
    }
  },
  {
    $group: {
      _id: "$claim.denial_reason",
      denied_claims: { $sum: 1 },
      total_billed_denied: { $sum: "$amounts.billed_amount" },
      avg_billed_denied: { $avg: "$amounts.billed_amount" }
    }
  },
  {
    $project: {
      _id: 0,
      denial_reason: "$_id",
      denied_claims: 1,
      total_billed_denied: { $round: ["$total_billed_denied", 2] },
      avg_billed_denied: { $round: ["$avg_billed_denied", 2] }
    }
  },
  {
    $sort: {
      denied_claims: -1
    }
  }
]);
```

5. March billing report
Billing report for March 2025 with overall and average billed/paid, total billed/paid by status and statistics on used payment method.

```
db.claims.aggregate([
  {
    $match: {
      "claim.claim_billing_date": {
        $gte: ISODate("2025-03-01T00:00:00.000Z"),
        $lt: ISODate("2025-04-01T00:00:00.000Z")
      }
    }
  },
  {
    $facet: {
      overall: [
        {
          $group: {
            _id: null,
            claim_count: { $sum: 1 },
            total_billed: { $sum: "$amounts.billed_amount" },
            total_paid: { $sum: "$amounts.paid_amount" },
            avg_billed: { $avg: "$amounts.billed_amount" },
            avg_paid: { $avg: "$amounts.paid_amount" }
          }
        },
        {
          $project: {
            _id: 0,
            claim_count: 1,
            total_billed: { $round: ["$total_billed", 2] },
            total_paid: { $round: ["$total_paid", 2] },
            avg_billed: { $round: ["$avg_billed", 2] },
            avg_paid: { $round: ["$avg_paid", 2] }
          }
        }
      ],
      by_status: [
        {
          $match: {
            "claim.claim_status": { $ne: null }
          }
        },
        {
          $group: {
            _id: "$claim.claim_status",
            claim_count: { $sum: 1 },
            total_billed: { $sum: "$amounts.billed_amount" },
            total_paid: { $sum: "$amounts.paid_amount" }
          }
        },
        {
          $project: {
            _id: 0,
            claim_status: "$_id",
            claim_count: 1,
            total_billed: { $round: ["$total_billed", 2] },
            total_paid: { $round: ["$total_paid", 2] }
          }
        },
        {
          $sort: {
            claim_count: -1,
            claim_status: 1
          }
        }
      ],
      top_payment_methods: [
        {
          $group: {
            _id: "$payment_method",
            claim_count: { $sum: 1 },
            total_paid: { $sum: "$amounts.paid_amount" }
          }
        },
        {
          $project: {
            _id: 0,
            payment_method: "$_id",
            claim_count: 1,
            total_paid: { $round: ["$total_paid", 2] }
          }
        },
        {
          $sort: {
            total_paid: -1,
            payment_method: 1
          }
        },
        {
          $limit: 5
        }
      ]
    }
  }
]);
```

6. Diagnosis diversity by department 
How many distinct diagnosis codes appear in each department, together with these diagnosis codes and the total number of encounters in department.

```
db.encounters.aggregate([
  {
    $group: {
      _id: "$department",
      encounter_count: { $sum: 1 },
      distinct_diagnosis_codes: { $addToSet: "$diagnosis_code" }
    }
  },
  {
    $project: {
      _id: 0,
      department: "$_id",
      encounter_count: 1,
      distinct_diagnosis_count: { $size: "$distinct_diagnosis_codes" },
      diagnosis_codes: "$distinct_diagnosis_codes"
    }
  },
  {
    $sort: {
      distinct_diagnosis_count: -1,
      encounter_count: -1,
      department: 1
    }
  }
]);
```

other ideas:
- denial rate per provider

## Join
1. Claims + patients + encounters
Denied claims on 31.3.2025 with patient demographics and encounter department, including the count of denied claims and the total denied amount per patient group.

```
db.claims.agregate([
  {
    $match: {
      "claim.claim_status": "Denied",
      "claim.claim_billing_date": {
        $gte: ISODate("2025-03-31T00:00:00.000Z"),
        $lt: ISODate("2025-04-01T00:00:00.000Z")
      }
    }
  },
  {
    $lookup: {
      from: "patients",
      localField: "patient_id",
      foreignField: "patient_id",
      as: "patient"
    }
  },
  {
    $unwind: "$patient"
  },
  {
    $lookup: {
      from: "encounters",
      localField: "encounter_id",
      foreignField: "encounter_id",
      as: "encounter"
    }
  },
  {
    $unwind: "$encounter"
  },
  {
    $addFields: {
      age_group: {
        $switch: {
          branches: [
            { case: { $lt: ["$patient.age", 18] }, then: "0-17" },
            { case: { $lt: ["$patient.age", 35] }, then: "18-34" },
            { case: { $lt: ["$patient.age", 50] }, then: "35-49" },
            { case: { $lt: ["$patient.age", 65] }, then: "50-64" }
          ],
          default: "65+"
        }
      }
    }
  },
  {
    $group: {
      _id: {
        department: "$encounter.department",
        gender: "$patient.gender",
        marital_status: "$patient.marital_status",
        age_group: "$age_group"
      },
      denied_claim_count: { $sum: 1 },
      total_billed_denied: { $sum: "$amounts.billed_amount" }    }
  },
  {
    $project: {
      _id: 0,
      department: "$_id.department",
      gender: "$_id.gender",
      marital_status: "$_id.marital_status",
      age_group: "$_id.age_group",
      denied_claim_count: 1,
      total_billed_denied: { $round: ["$total_billed_denied", 2] }    }
  },
  {
    $sort: {
      denied_claim_count: -1,
      total_billed_denied: -1
    }
  }
])
```
2.  Claims + encounters
Departments that have had the most expensive claims (top 100) with their billed and paid financial values

```
db.claims.aggregate([
  {
    $sort: {
      "amounts.billed_amount": -1
    }
  },
  {
    $limit: 100
  },
  {
    $lookup: {
      from: "encounters",
      localField: "encounter_id",
      foreignField: "encounter_id",
      as: "encounter"
    }
  },
  {
    $unwind: "$encounter"
  },
  {
    $group: {
      _id: "$encounter.department",
      claim_count: { $sum: 1 },
      total_billed_amount: { $sum: "$amounts.billed_amount" },
      total_paid_amount: { $sum: "$amounts.paid_amount" }
    }
  },
  {
    $project: {
      _id: 0,
      department: "$_id",
      claim_count: 1,
      total_billed_amount: { $round: ["$total_billed_amount", 2] },
      total_paid_amount: { $round: ["$total_paid_amount", 2]}
    }
  },
  {
    $sort: {
      total_billed_amount: -1,
      claim_count: -1
    }
  }
])
```


3. Claims + patients
Top 10 billed patients of Medicare insurance provider in March 2025
Filters claims first, sorts by billed amount, limits to 10, then joins only those records with patients and projects the patient information, billed and paid amount.

```
db.claims.aggregate([
  {
    $match: {
      insurance_provider: "Medicare",
      "claim.claim_billing_date": {
        $gte: ISODate("2025-03-01T00:00:00.000Z"),
        $lt: ISODate("2025-04-01T00:00:00.000Z")
      }
    }
  },
  { $sort: { "amounts.billed_amount": -1 } },
  { $limit: 10 },
  {
    $lookup: {
      from: "patients",
      localField: "patient_id",
      foreignField: "patient_id",
      as: "patient"
    }
  },
  { $unwind: "$patient" },
  {
    $project: {
      _id: 0,
      patient_name: { $concat: ["$patient.first_name", " ", "$patient.last_name"] },
      age: "$patient.age",
      gender: "$patient.gender",
      total_billed_amount: { $round: ["$amounts.billed_amount", 2] },
      total_paid_amount: { $round: ["$amounts.paid_amount", 2] }
    }
  },
  { $sort: { total_billed_amount: -1 } }
])
```

4. Encounters + patients
March 2025 emergency visits across patient insurance types, including both total emergency encounters, unique patients counts and average patient age.

```
db.encounters.aggregate([
  {
    $match: {
      visit_type: "Emergency",
      visit_date: {
        $gte: ISODate("2025-03-01T00:00:00.000Z"),
        $lt: ISODate("2025-04-01T00:00:00.000Z")
      }
    }
  },
  {
    $lookup: {
      from: "patients",
      localField: "patient_id",
      foreignField: "patient_id",
      as: "patient"
    }
  },
  {
    $unwind: "$patient"
  },
  {
    $group: {
      _id: "$patient.insurance_type",
      emergency_encounter_count: { $sum: 1 },
      unique_patient_ids: { $addToSet: "$patient_id" },
      avg_patient_age: { $avg: "$patient.age" }
    }
  },
  {
    $project: {
      _id: 0,
      insurance_type: "$_id",
      emergency_encounter_count: 1,
      unique_patient_count: { $size: "$unique_patient_ids" },
      avg_patient_age: { $round: ["$avg_patient_age", 2] }
    }
  },
  {
    $sort: {
      emergency_encounter_count: -1
    }
  }
])
```

5. Injury-related visits by patient residence state in March 2025
patients state + the reason for visit is connected to injury 
group by state, project to patient count

The query filters encounters to Accidental Injury visits first, then joins matching encounters with patients to access the nested field patient.contact.state. It groups by state and returns encounter count, unique patient count, and sorts by injury encounter count. 

```
db.encounters.aggregate([
  {
    $match: {
      reason_for_visit: "Accidental Injury",
      visit_date: {
        $gte: ISODate("2025-03-01T00:00:00.000Z"),
        $lt: ISODate("2025-04-01T00:00:00.000Z")
      }
    }
  },
  {
    $lookup: {
      from: "patients",
      localField: "patient_id",
      foreignField: "patient_id",
      as: "patient"
    }
  },
  {
    $unwind: "$patient"
  },
  {
    $group: {
      _id: "$patient.contact.state",
      injury_encounter_count: { $sum: 1 },
      unique_patient_ids: { $addToSet: "$patient_id" }
    }
  },
  {
    $project: {
      _id: 0,
      state: "$_id",
      injury_encounter_count: 1,
      unique_patient_count: { $size: "$unique_patient_ids" }    }
  },
  {
    $sort: {
      injury_encounter_count: -1,
      state: 1
    }
  }
])
```


6. Encounters + claims
Newborn claims stats

This query summarizes billing outcomes for newborn admissions, including how many newborn encounters exist, how many related claims were denied, and the minimum, maximum, and average billed amount.

Technical explanation:
The query filters encounters to admission.admission_type = "Newborn", joins matching claims using encounter_id, and aggregates claim statistics over the joined records.
```
db.encounters.aggregate([
  {
    $match: {
      "admission.admission_type": "Newborn"
    }
  },
  {
    $lookup: {
      from: "claims",
      localField: "encounter_id",
      foreignField: "encounter_id",
      as: "claim"
    }
  },
  {
    $unwind: "$claim"
  },
  {
    $group: {
      _id: null,
      newborn_encounter_count: { $sum: 1 },
      denied_claim_count: {
        $sum: {
          $cond: [
            { $eq: ["$claim.claim.claim_status", "Denied"] },
            1,
            0
          ]
        }
      },
      min_billed_amount: { $min: "$claim.amounts.billed_amount" },
      max_billed_amount: { $max: "$claim.amounts.billed_amount" },
      avg_billed_amount: { $avg: "$claim.amounts.billed_amount" }
    }
  },
  {
    $project: {
      _id: 0,
      newborn_encounter_count: 1,
      denied_claim_count: 1,
      min_billed_amount: { $round: ["$min_billed_amount", 2] },
      max_billed_amount: { $round: ["$max_billed_amount", 2] },
      avg_billed_amount: { $round: ["$avg_billed_amount", 2] }
    }
  }
]);
```


## Embedded documenty

1. Patients with missing both phone and email by insurance type
Find the count of patients with missing contact information (both email and phone) by their insurance type. 
This checks if there is any trend with missing information through the insurance customers.

```
db.patients.aggregate(
[
  {
    $match: {
      "contact.email": { $in: [null, ""] },
      "contact.phone": { $in: [null, ""] }
    }
  },
  {
    $group: {
      _id: "$insurance_type",
      no_contact_information: {
        $sum: 1
      }
    }
  },
  {
    $sort: {
      no_contact_information: -1
    }
  },
  {
    $project: {
      _id: 0,
      insurance_type: "$_id",
      no_contact_information: 1
    }
  }
])
```

2. Patients with incomplete nested address data

```
db.patients.find(
  {
    $or: [
      { "contact.address": { $in: [null, ""] } },
      { "contact.city": { $in: [null, ""] } }
    ]
  }
).sort({ patient_id: 1 });
```

3. Latest Maternity admissions encounters with maximum admission stay, sorted by longest stay

```
db.encounters.find(
  {
    "admission.admission_type": "Maternity",
    "admission.discharge_date": { $ne: null },
    "admission.length_of_stay": { $ne: null }
  },
  {
    _id: 0,
    encounter_id: 1,
    patient_id: 1,
    provider_id: 1,
    visit_date: 1,
    department: 1,
    reason_for_visit: 1,
    diagnosis_code: 1,
    readmitted_flag: 1,
    "admission.discharge_date": 1,
    "admission.length_of_stay": 1
  }
)
.sort({
  "admission.length_of_stay": -1,
  "admission.discharge_date": -1,
})
.limit(10);
```

4. Find denied claims for specific day (30.03.2025) with patient, encounter and claim id, payment method, denial reason, and billed amount.

```
db.claims.find(
  {
    "claim.claim_status": "Denied",
    "claim.claim_billing_date": {
      $gte: ISODate("2025-03-30T00:00:00.000Z"),
      $lt: ISODate("2025-03-31T00:00:00.000Z")
    }
  },
  {
    _id: 0,
    patient_id: 1,
    encounter_id: 1, 
    payment_method: 1, 
    "claim.claim_id": 1, 
    "claim.denial_reason": 1,
    "amounts.billed_amount": 1
  }
)
```

5. Paid amount < Billed amount
Find paid claims where the paid amount is lower than the billed amount
```
db.claims.find(
  {
    "claim.claim_status": "Paid",
    $expr: {
      $lt: ["$amounts.paid_amount", "$amounts.billed_amount"]
    }
  },
  {
    _id: 0,
    billing_id: 1,
    patient_id: 1,
    encounter_id: 1,
    insurance_provider: 1,
    payment_method: 1,
    "claim.claim_id": 1,
    "claim.claim_billing_date": 1,
    "amounts.billed_amount": 1,
    "amounts.paid_amount": 1
  }
).sort({ "claim.claim_billing_date": -1, billing_id: 1 });
```

6. Claims with missing nested claim metadata
Either claim.claim_id or claim.claim_billing_date missing

```
db.claims.find(
  {
    $or: [
      { "claim.claim_id": { $in: [null, ""] } },
      { "claim.claim_billing_date": null }
    ]
  },
  {
    _id: 0,
    billing_id: 1,
    patient_id: 1,
    encounter_id: 1,
    insurance_provider: 1,
    payment_method: 1,
    "claim.claim_id": 1,
    "claim.claim_billing_date": 1,
    "claim.claim_status": 1,
    "claim.denial_reason": 1,
    "amounts.billed_amount": 1,
    "amounts.paid_amount": 1
  }
).sort({ billing_id: 1 });
```



## CRUD - insert, update, delete, merge
1. Add new test patient

```
db.patients.insertOne({
  patient_id: "TEST_PATIENT_001",
  first_name: "Test",
  last_name: "Readmission",
  dob: ISODate("1985-05-12T00:00:00.000Z"),
  age: 39,
  gender: "Female",
  ethnicity: "Test",
  insurance_type: "Medicare",
  marital_status: "Single",
  contact: {
    address: "100 Test Street",
    city: "Los Angeles",
    state: "CA",
    zip: "90001",
    phone: "555-0100",
    email: "test.readmission@example.com"
  },
  registration_date: ISODate("2025-03-15T00:00:00.000Z")
});
```

2. Insert a test encounter for test patient

```
db.encounters.insertOne({
  encounter_id: "TEST_ENCOUNTER_PREVIOUS_001",
  patient_id: "TEST_PATIENT_001",
  provider_id: "TEST_PROVIDER_001",
  visit_date: ISODate("2025-03-20T09:00:00.000Z"),
  visit_type: "Emergency",
  department: "Cardiology",
  reason_for_visit: "Chest pain",
  diagnosis_code: "TEST-DX-001",
  admission: {
    admission_type: null,
    discharge_date: null,
    length_of_stay: null
  },
  status: "Completed",
  readmitted_flag: false
});
```

3. Insert a claim for test encounter and patient

```
db.claims.insertOne({
  billing_id: "TEST_BILLING_001",
  patient_id: "TEST_PATIENT_001",
  encounter_id: "TEST_ENCOUNTER_PREVIOUS_001",
  insurance_provider: "Medicare",
  payment_method: "Insurance",
  claim: {
    claim_id: "TEST_CLAIM_001",
    claim_billing_date: ISODate("2025-03-21T00:00:00.000Z"),
    claim_status: "Paid",
    denial_reason: null
  },
  amounts: {
    billed_amount: NumberDecimal("1850.00"),
    paid_amount: NumberDecimal("1500.00")
  }
});
```

4. Delete demo records with regex match
```
db.claims.deleteMany({
  billing_id: /^TEST_/
});

db.encounters.deleteMany({
  encounter_id: /^TEST_/
});

db.patients.deleteMany({
  patient_id: /^TEST_/
});
```

5. Data correction -> if underage patient has an irrelevant marital status (e.g. married or divorced), 
set the marital status to "Needs review"

This query finds underage patients whose marital status is either "Married" or "Widowed/Divorced/Separated" and updates them to "Needs Review" for data-quality control.

```
db.patients.updateMany(
  {
    age: { $lt: 18 },
    marital_status: {
      $in: ["Married", "Widowed/Divorced/Separated"]
    }
  },
  {
    $set: {
      marital_status: "Needs Review"
    }
  }
);
```


6. Create a separate collection for monthly claim summary using merge 

```
db.claims.aggregate([
  {
    $match: {
      "claim.claim_billing_date": {$ne: null}
    }
  },
  {
    $group: {
      _id: {
        month: {
          $dateToString: {
            format: "%Y-%m",
            date: "$claim.claim_billing_date"
          }
        },
        claim_status: "$claim.claim_status"
      },
      claim_count: { $sum: 1 },
      total_billed_amount: { $sum: "$amounts.billed_amount" },
      total_paid_amount: { $sum: "$amounts.paid_amount" },
      avg_billed_amount: { $avg: "$amounts.billed_amount" },
      avg_paid_amount: { $avg: "$amounts.paid_amount" }
    }
  },
  {
    $project: {
      _id: {
         $concat: ["$_id.month", " - ", "$_id.claim_status"] 
      },
      month: "$_id.month",
      claim_status: "$_id.claim_status",
      claim_count: 1,
      total_billed_amount: { $round: ["$total_billed_amount", 2] },
      total_paid_amount: { $round: ["$total_paid_amount", 2] },
      avg_billed_amount: { $round: ["$avg_billed_amount", 2] },
      avg_paid_amount: { $round: ["$avg_paid_amount", 2] }
    }
  },
  {
    $sort: {
      month: 1
    }
  },
  {
    $merge: {
      into: "monthly_claim_status_summary",
      on: "_id",
      whenMatched: "replace",
      whenNotMatched: "insert"
    }
  }
])
```
## Indexes, Sharding, Replication, Cluster, Configs