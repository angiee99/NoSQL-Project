## Aggreagation 
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
      denied_claims: -1,
      denial_reason: 1
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