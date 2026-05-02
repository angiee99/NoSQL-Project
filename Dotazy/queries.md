# Dotazy

Dotazy pracují se třemi kolekcemi `patients`, `encounters` a `claims`.

Před spuštěním dotazů je vhodné přepnout se do projektové databáze:

```js
use projectdb;
```
Tato část je v následujících příkazech vynechána.

## 1. Agregační dotazy

### 1.1 Průměrné částky podle stavu pojistného nároku

**Zadání:** Zjistěte průměrnou účtovanou částku, průměrnou zaplacenou částku a počet záznamů pro jednotlivé stavy pojistných nároků - `Paid` a `Denied`.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
  {
    $match: {
      "claim.claim_status": { $ne: null }
    }
  },
  {
    $group: {
      _id: "$claim.claim_status",
      avg_billed: { $avg: "$amounts.billed_amount" },
      avg_paid: { $avg: "$amounts.paid_amount" },
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
  {
    $sort: { claim_count: -1 }
  }
]);
```

### 1.2 Nejčastější typ pojištění podle státu

**Zadání:** Pro každý stát zjistěte, který typ pojištění je mezi pacienty nejčastější, a vraťte také počet pacientů s tímto typem pojištění.

**Řešení v MongoDB:**

```js
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
]);
```

### 1.3 Délka urgentní hospitalizace a míra readmise

**Zadání:** Analyzujte urgentní hospitalizace podle délky pobytu. Pro každou délku pobytu spočítejte počet urgentních návštěv, počet readmisí a procentuální míru readmise.

**Řešení v MongoDB:**

```js
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

### 1.4 Finanční analýza zamítnutých nároků podle důvodu zamítnutí

**Zadání:** Zjistěte, které důvody zamítnutí pojistných nároků jsou nejčastější a jaká celková i průměrná účtovaná částka je s nimi spojena.

**Řešení v MongoDB:**

```js
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
    $sort: { denied_claims: -1 }
  }
]);
```

### 1.5 Měsíční billing report za březen 2025

**Zadání:** Vytvořte souhrnný report pojistných nároků za březen 2025. Report má obsahovat celkový přehled, rozpad podle stavu nároku a nejvýznamnější platební metody podle zaplacené částky.

**Řešení v MongoDB:**

```js
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
      by_payment_methods: [
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
        }
      ]
    }
  }
]);
```

### 1.6 Rozmanitost diagnóz podle oddělení

**Zadání:** Pro každé nemocniční oddělení zjistěte počet návštěv, počet unikátních diagnóz a seznam kódů diagnóz, které se na daném oddělení vyskytují.

**Řešení v MongoDB:**

```js
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

---

## 2. Dotazy nad propojenými kolekcemi

### 2.1 Zamítnuté nároky s demografickými údaji pacienta a oddělením

**Zadání:** Pro zamítnuté pojistné nároky ze dne 31. 3. 2025 propojte kolekce `claims`, `patients` a `encounters`. Výsledky seskupte podle oddělení, pohlaví a věkové skupiny pacienta.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
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
        age_group: "$age_group"
      },
      denied_claim_count: { $sum: 1 },
      total_billed_denied: { $sum: "$amounts.billed_amount" }
    }
  },
  {
    $project: {
      _id: 0,
      department: "$_id.department",
      gender: "$_id.gender",
      age_group: "$_id.age_group",
      denied_claim_count: 1,
      total_billed_denied: { $round: ["$total_billed_denied", 2] }
    }
  },
  {
    $sort: {
      denied_claim_count: -1,
      total_billed_denied: -1
    }
  }
]);
```

### 2.2 Oddělení s nejdražšími pojistnými nároky

**Zadání:** Najděte 100 nejdražších pojistných nároků podle účtované částky, propojte je s návštěvami a zjistěte, na která oddělení tyto nároky připadají.

**Řešení v MongoDB:**

```js
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
      total_paid_amount: { $round: ["$total_paid_amount", 2] }
    }
  },
  {
    $sort: {
      total_billed_amount: -1,
      claim_count: -1
    }
  }
]);
```

### 2.3 Top 10 pacientů pojištěných přes Medicare podle účtované částky

**Zadání:** Najděte deset nejvyšších pojistných nároků za březen 2025 u pojišťovny Medicare a doplňte k nim základní údaje o pacientech.

**Řešení v MongoDB:**

```js
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
  {
    $sort: { "amounts.billed_amount": -1 }
  },
  {
    $limit: 10
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
    $project: {
      _id: 0,
      patient_id: 1,
      patient_name: { $concat: ["$patient.first_name", " ", "$patient.last_name"] },
      age: "$patient.age",
      gender: "$patient.gender",
      total_billed_amount: { $round: ["$amounts.billed_amount", 2] },
      total_paid_amount: { $round: ["$amounts.paid_amount", 2] }
    }
  },
  {
    $sort: { total_billed_amount: -1 }
  }
]);
```

### 2.4 Pohotovostní návštěvy podle typu pojištění pacienta

**Zadání:** Pro pohotovostní návštěvy v březnu 2025 zjistěte počet návštěv, počet unikátních pacientů a průměrný věk pacientů podle typu jejich pojištění.

**Řešení v MongoDB:**

```js
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
]);
```

### 2.5 Úrazy podle státu bydliště pacienta

**Zadání:** Pro návštěvy související s úrazem v březnu 2025 zjistěte, ze kterých států pacienti pocházejí. Výsledek má obsahovat počet návštěv a počet unikátních pacientů podle státu.

**Řešení v MongoDB:**

```js
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
      unique_patient_count: { $size: "$unique_patient_ids" }
    }
  },
  {
    $sort: {
      injury_encounter_count: -1,
      state: 1
    }
  }
]);
```

### 2.6 Statistiky pojistných nároků u novorozeneckých hospitalizací

**Zadání:** Pro hospitalizace typu `Newborn` zjistěte počet souvisejících pojistných nároků, počet zamítnutých nároků a minimální, maximální a průměrnou účtovanou částku.

**Řešení v MongoDB:**

```js
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
      newborn_claim_count: { $sum: 1 },
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
      newborn_claim_count: 1,
      denied_claim_count: 1,
      min_billed_amount: { $round: ["$min_billed_amount", 2] },
      max_billed_amount: { $round: ["$max_billed_amount", 2] },
      avg_billed_amount: { $round: ["$avg_billed_amount", 2] }
    }
  }
]);
```

---

## 3. Dotazy nad vnořenými dokumenty

### 3.1 Pacienti bez e-mailu i telefonu podle typu pojištění

**Zadání:** Zjistěte, kolik pacientů nemá vyplněný telefon ani e-mail. Výsledek seskupte podle typu pojištění.

**Řešení v MongoDB:**

```js
db.patients.aggregate([
  {
    $match: {
      "contact.email": { $in: [null, ""] },
      "contact.phone": { $in: [null, ""] }
    }
  },
  {
    $group: {
      _id: "$insurance_type",
      no_contact_information: { $sum: 1 }
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
]);
```

### 3.2 Pacienti s pojištěním Aetna a neúplnou adresou

**Zadání:** Zjistěte, jak neúplné adresní údaje (město a samotná adresa) se vyskytují u pacientů s pojištěním Aetna. Výsledek seskupte podle chybějících polí, uveďte počet dotčených pacientů, jejich podíl ze všech pacientů s pojištěním Aetna a průměrný věk v dané skupině.

**Řešení v MongoDB:**

```js
db.patients.aggregate([
  {
    $match: {
      insurance_type: "Aetna"
    }
  },
  {
    $addFields: {
      is_missing_address: {
        $eq: [
          { $trim: { input: { $ifNull: ["$contact.address", ""] } } },
          ""
        ]
      },
      is_missing_city: {
        $eq: [
          { $trim: { input: { $ifNull: ["$contact.city", ""] } } },
          ""
        ]
      }
    }
  },
  {
    $addFields: {
      missing_address_fields: {
        $concatArrays: [
          {
            $cond: [
              "$is_missing_address",
              ["contact.address"],
              []
            ]
          },
          {
            $cond: [
              "$is_missing_city",
              ["contact.city"],
              []
            ]
          }
        ]
      }
    }
  },
  {
    $facet: {
      total_aetna: [
        {
          $count: "total_patients"
        }
      ],
      incomplete_address_summary: [
        {
          $match: {
            $expr: {
              $gt: [
                { $size: "$missing_address_fields" },
                0
              ]
            }
          }
        },
        {
          $group: {
            _id: "$missing_address_fields",
            affected_patients: { $sum: 1 },
            avg_age: { $avg: "$age" }
          }
        },
        {
          $project: {
            _id: 0,
            missing_fields: "$_id",
            affected_patients: 1,
            avg_age: { $round: ["$avg_age", 2] }
          }
        },
        {
          $sort: {
            affected_patients: -1
          }
        }
      ]
    }
  },
  {
    $unwind: "$total_aetna"
  },
  {
    $unwind: "$incomplete_address_summary"
  },
  {
    $project: {
      _id: 0,
      missing_fields: "$incomplete_address_summary.missing_fields",
      affected_patients: "$incomplete_address_summary.affected_patients",
      total_aetna_patients: "$total_aetna.total_patients",
      share_of_aetna_pct: {
        $round: [
          {
            $multiply: [
              {
                $divide: [
                  "$incomplete_address_summary.affected_patients",
                  "$total_aetna.total_patients"
                ]
              },
              100
            ]
          },
          2
        ]
      },
      avg_age: "$incomplete_address_summary.avg_age"
    }
  },
  {
    $sort: {
      affected_patients: -1
    }
  }
]);
```

### 3.3 Analýza mateřských hospitalizací podle měsíce propuštění

**Zadání:** Analyzujte hospitalizace typu Maternity podle měsíce hospitalizace. Pro každý měsíc zobrazte počet hospitalizací, počet unikátních pacientů, průměrnou délku hospitalizace, rozdělení hospitalizací podle počtu dnů, počet readmisí a procentuální míru readmise.

**Řešení v MongoDB:**

```js
db.encounters.aggregate([
  {
    $match: {
      "admission.admission_type": "Maternity",
      "admission.discharge_date": { $ne: null },
      "admission.length_of_stay": { $in: [1, 2, 3] }
    }
  },
  {
    $addFields: {
      discharge_month: {
        $dateToString: {
          format: "%Y-%m",
          date: "$admission.discharge_date"
        }
      }
    }
  },
  {
    $group: {
      _id: "$discharge_month",
      maternity_encounters: { $sum: 1 },
      unique_patient_ids: { $addToSet: "$patient_id" },
      avg_length_of_stay: { $avg: "$admission.length_of_stay" },

      one_day_stays: {
        $sum: {
          $cond: [
            { $eq: ["$admission.length_of_stay", 1] },
            1,
            0
          ]
        }
      },
      two_day_stays: {
        $sum: {
          $cond: [
            { $eq: ["$admission.length_of_stay", 2] },
            1,
            0
          ]
        }
      },
      three_day_stays: {
        $sum: {
          $cond: [
            { $eq: ["$admission.length_of_stay", 3] },
            1,
            0
          ]
        }
      },

      readmitted_cases: {
        $sum: {
          $cond: [
            { $eq: ["$readmitted_flag", true] },
            1,
            0
          ]
        }
      }
    }
  },
  {
    $addFields: {
      unique_patient_count: {
        $size: "$unique_patient_ids"
      },
      readmission_rate_pct: {
        $round: [
          {
            $multiply: [
              {
                $divide: ["$readmitted_cases", "$maternity_encounters"]
              },
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
      discharge_month: "$_id",
      maternity_encounters: 1,
      unique_patient_count: 1,
      avg_length_of_stay: {
        $round: ["$avg_length_of_stay", 2]
      },
      one_day_stays: 1,
      two_day_stays: 1,
      three_day_stays: 1,
      readmitted_cases: 1,
      readmission_rate_pct: 1
    }
  },
  {
    $sort: {
      discharge_month: 1
    }
  }
]);
```

### 3.4 Zamítnuté pojistné nároky za konkrétní den

**Zadání:** Analyzujte zamítnuté pojistné nároky ze dne 30. 3. 2025. Výsledek seskupte podle důvodu zamítnutí, pojišťovny a platební metody. Pro každou skupinu zobrazte počet zamítnutých nároků, celkovou účtovanou částku zamítnutých nároků, průměrnou účtovanou částku a nejvyšší účtovanou částku.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
  {
    $match: {
      "claim.claim_status": "Denied",
      "claim.claim_billing_date": {
        $gte: ISODate("2025-03-30T00:00:00.000Z"),
        $lt: ISODate("2025-03-31T00:00:00.000Z")
      },
      "claim.denial_reason": {
        $nin: [null, ""]
      }
    }
  },
  {
    $group: {
      _id: {
        denial_reason: "$claim.denial_reason",
        insurance_provider: "$insurance_provider",
        payment_method: "$payment_method"
      },
      denied_claim_count: {
        $sum: 1
      },
      total_denied_billed_amount: {
        $sum: "$amounts.billed_amount"
      },
      avg_billed_amount: {
        $avg: "$amounts.billed_amount"
      },
      max_billed_amount: {
        $max: "$amounts.billed_amount"
      }
    }
  },
  {
    $project: {
      _id: 0,
      denial_reason: "$_id.denial_reason",
      insurance_provider: "$_id.insurance_provider",
      payment_method: "$_id.payment_method",
      denied_claim_count: 1,
      total_denied_billed_amount: {
        $round: ["$total_denied_billed_amount", 2]
      },
      avg_billed_amount: {
        $round: ["$avg_billed_amount", 2]
      },
      max_billed_amount: {
        $round: ["$max_billed_amount", 2]
      }
    }
  },
  {
    $sort: {
      total_denied_billed_amount: -1,
      denied_claim_count: -1
    }
  }
]);
```

### 3.5 Analýza částečně zaplacených pojistných nároků

**Zadání:** Analyzujte pojistné nároky se stavem `Paid`, u kterých je zaplacená částka nižší než účtovaná částka. Výsledek seskupte podle pojišťovny a platební metody. Pro každou skupinu zobrazte počet částečně zaplacených nároků, celkovou účtovanou částku, celkovou zaplacenou částku, celkový nedoplatek, průměrný nedoplatek a průměrné procento zaplacení.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
  {
    $match: {
      "claim.claim_status": "Paid",
      $expr: {
        $lt: [
          "$amounts.paid_amount",
          "$amounts.billed_amount"
        ]
      }
    }
  },
  {
    $addFields: {
      underpaid_amount: {
        $subtract: [
          "$amounts.billed_amount",
          "$amounts.paid_amount"
        ]
      },
      paid_ratio_pct: {
        $multiply: [
          {
            $divide: [
              "$amounts.paid_amount",
              "$amounts.billed_amount"
            ]
          },
          100
        ]
      }
    }
  },
  {
    $group: {
      _id: {
        insurance_provider: "$insurance_provider",
        payment_method: "$payment_method"
      },
      partially_paid_claims: {
        $sum: 1
      },
      total_billed_amount: {
        $sum: "$amounts.billed_amount"
      },
      total_paid_amount: {
        $sum: "$amounts.paid_amount"
      },
      total_underpaid_amount: {
        $sum: "$underpaid_amount"
      },
      avg_underpaid_amount: {
        $avg: "$underpaid_amount"
      },
      avg_paid_ratio_pct: {
        $avg: "$paid_ratio_pct"
      },
      max_underpaid_amount: {
        $max: "$underpaid_amount"
      }
    }
  },
  {
    $project: {
      _id: 0,
      insurance_provider: "$_id.insurance_provider",
      payment_method: "$_id.payment_method",
      partially_paid_claims: 1,
      total_billed_amount: {
        $round: ["$total_billed_amount", 2]
      },
      total_paid_amount: {
        $round: ["$total_paid_amount", 2]
      },
      total_underpaid_amount: {
        $round: ["$total_underpaid_amount", 2]
      },
      avg_underpaid_amount: {
        $round: ["$avg_underpaid_amount", 2]
      },
      avg_paid_ratio_pct: {
        $round: ["$avg_paid_ratio_pct", 2]
      },
      max_underpaid_amount: {
        $round: ["$max_underpaid_amount", 2]
      }
    }
  },
  {
    $sort: {
      total_underpaid_amount: -1,
      partially_paid_claims: -1
    }
  }
]);
```

### 3.6 Přehled fakturací podle platební metody

**Zadání:** Porovnejte fakturace podle platební metody Selfpay a Insurance. Pro každou platební metodu zobrazte počet fakturací, procentuální podíl ze všech fakturací, celkovou účtovanou částku, celkovou zaplacenou částku a procento uhrazení.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
  {
    $facet: {
      total: [
        {
          $group: {
            _id: null,
            total_billings: { $sum: 1 }
          }
        }
      ],
      by_payment_method: [
        {
          $group: {
            _id: "$payment_method",
            billing_count: { $sum: 1 },
            total_billed_amount: {
              $sum: "$amounts.billed_amount"
            },
            total_paid_amount: {
              $sum: "$amounts.paid_amount"
            }
          }
        }
      ]
    }
  },
  {
    $unwind: "$total"
  },
  {
    $unwind: "$by_payment_method"
  },
  {
    $project: {
      _id: 0,
      payment_method: "$by_payment_method._id",
      billing_count: "$by_payment_method.billing_count",
      share_of_billings_pct: {
        $round: [
          {
            $multiply: [
              {
                $divide: [
                  "$by_payment_method.billing_count",
                  "$total.total_billings"
                ]
              },
              100
            ]
          },
          2
        ]
      },
      total_billed_amount: {
        $round: ["$by_payment_method.total_billed_amount", 2]
      },
      total_paid_amount: {
        $round: ["$by_payment_method.total_paid_amount", 2]
      },
      paid_to_billed_pct: {
        $round: [
          {
            $multiply: [
              {
                $divide: [
                  "$by_payment_method.total_paid_amount",
                  "$by_payment_method.total_billed_amount"
                ]
              },
              100
            ]
          },
          2
        ]
      }
    }
  },
  {
    $sort: {
      billing_count: -1
    }
  }
]
);
```

---

## 4. Práce s daty: insert, update, delete a merge

### 4.1 Vložení testovacího pacienta

**Zadání:** Vložte do kolekce `patients` nového testovacího pacienta a následně ověřte, že byl dokument uložen.

**Řešení v MongoDB:**

```js
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

db.patients.find({ patient_id: "TEST_PATIENT_001" });
```

### 4.2 Vložení testovací návštěvy pro testovacího pacienta

**Zadání:** Pro testovacího pacienta vložte záznam o urgentní návštěvě a následně ověřte, že byl dokument uložen v kolekci `encounters`.

**Řešení v MongoDB:**

```js
db.encounters.insertOne({
  encounter_id: "TEST_ENCOUNTER_001",
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

db.encounters.find({ encounter_id: "TEST_ENCOUNTER_001" });
```

### 4.3 Vložení testovacího pojistného nároku

**Zadání:** Vložte pojistný nárok navázaný na testovacího pacienta a testovací návštěvu. Následně ověřte vložení podle `billing_id`.

**Řešení v MongoDB:**

```js
db.claims.insertOne({
  billing_id: "TEST_BILLING_001",
  patient_id: "TEST_PATIENT_001",
  encounter_id: "TEST_ENCOUNTER_001",
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

db.claims.find({ billing_id: "TEST_BILLING_001" });
```

### 4.4 Smazání testovacích dat

**Zadání:** Smažte všechny testovací záznamy vytvořené v předchozích dotazech a ověřte, že už v kolekcích nejsou žádné dokumenty s prefixem `TEST_`.

**Řešení v MongoDB:**

```js
db.claims.deleteMany({
  billing_id: /^TEST_/
});

db.encounters.deleteMany({
  encounter_id: /^TEST_/
});

db.patients.deleteMany({
  patient_id: /^TEST_/
});

db.claims.find({ billing_id: /^TEST_/ });
db.encounters.find({ encounter_id: /^TEST_/ });
db.patients.find({ patient_id: /^TEST_/ });
```

### 4.5 Oprava nekonzistentního rodinného stavu u nezletilých pacientů

**Zadání:** Najděte nezletilé pacienty, kteří mají uveden rodinný stav `Married` nebo `Widowed/Divorced/Separated`, a označte jejich rodinný stav hodnotou `Needs Review` pro další kontrolu kvality dat.

**Řešení v MongoDB:**

```js
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

db.patients.find(
  {
    age: { $lt: 18 },
    marital_status: "Needs Review"
  },
  {
    _id: 0,
    patient_id: 1,
    age: 1,
    marital_status: 1
  }
);
```

### 4.6 Vytvoření měsíční souhrnné kolekce pomocí `$merge`

**Zadání:** Vytvořte samostatnou kolekci `monthly_claim_status_summary`, která bude obsahovat měsíční souhrny pojistných nároků podle stavu nároku.

**Řešení v MongoDB:**

```js
db.claims.aggregate([
  {
    $match: {
      "claim.claim_billing_date": { $ne: null }
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
]);

db.monthly_claim_status_summary.find().sort({ month: 1, claim_status: 1 });
```

---

## 5. Indexy, sharding, replikace a cluster

### 5.1 Analýza dotazu bez vhodného indexu

**Zadání:** Spusťte dotaz nad kolekcí `claims`, který filtruje podle pojišťovny a data fakturace. Pomocí `explain("executionStats")` zjistěte, jak se dotaz provádí bez vynucení konkrétního indexu.

**Řešení v MongoDB:**

```js
db.claims.find(
  {
    insurance_provider: "Medicare",
    "claim.claim_billing_date": {
      $gte: ISODate("2025-03-01T00:00:00.000Z"),
      $lt: ISODate("2025-04-01T00:00:00.000Z")
    }
  }
)
.sort({ "claim.claim_billing_date": 1 })
.limit(10)
.explain("executionStats");
```

**Poznámka:** Tento dotaz slouží jako výchozí měření pro porovnání s dotazem, který použije složený index.

### 5.2 Vytvoření složeného indexu a spuštění dotazu s `hint`

**Zadání:** Vytvořte složený index nad poli `insurance_provider` a `claim.claim_billing_date`. Poté spusťte stejný dotaz s vynucením tohoto indexu pomocí `hint`.

**Řešení v MongoDB:**

```js
db.claims.createIndex(
  {
    insurance_provider: 1,
    "claim.claim_billing_date": 1
  },
  {
    name: "insurance_provider_1_claim_billing_date_1"
  }
);

db.claims.find(
  {
    insurance_provider: "Medicare",
    "claim.claim_billing_date": {
      $gte: ISODate("2025-03-01T00:00:00.000Z"),
      $lt: ISODate("2025-04-01T00:00:00.000Z")
    }
  }
)
.sort({ "claim.claim_billing_date": 1 })
.limit(10)
.hint("insurance_provider_1_claim_billing_date_1")
.explain("executionStats");
```

**Poznámka:** Výsledek `explain` umožňuje porovnat počet prohledaných klíčů, počet prohledaných dokumentů a použitý index.

### 5.3 Vynucené sekvenční čtení pomocí `$natural`

**Zadání:** Spusťte stejný dotaz znovu, ale tentokrát vynuťte přirozené čtení kolekce pomocí `hint({ $natural: 1 })`. Výsledek použijte pro srovnání s indexovaným dotazem.

**Řešení v MongoDB:**

```js
db.claims.find(
  {
    insurance_provider: "Medicare",
    "claim.claim_billing_date": {
      $gte: ISODate("2025-03-01T00:00:00.000Z"),
      $lt: ISODate("2025-04-01T00:00:00.000Z")
    }
  }
)
.sort({ "claim.claim_billing_date": 1 })
.limit(10)
.hint({ $natural: 1 })
.explain("executionStats");
```

**Poznámka:** Tento dotaz ukazuje rozdíl mezi použitím indexu a čtením dokumentů v přirozeném pořadí uložení.

### 5.4 Zobrazení distribuce dat mezi shardy

**Zadání:** Ověřte, jak jsou dokumenty a chunky rozdělené mezi jednotlivé shardy pro kolekce `patients`, `encounters` a `claims`.

**Řešení v MongoDB:**

```js
db.patients.getShardDistribution();
db.encounters.getShardDistribution();
db.claims.getShardDistribution();
```

**Poznámka:** Výstup ukazuje, kolik dat, dokumentů a chunků je umístěno na jednotlivých shardech. V tomto projektu jsou kolekce shardované podle hashovaného klíče `patient_id`, což podporuje rovnoměrnější rozložení záznamů mezi tři shardy.

### 5.5 Konfigurace shardingu a počet chunků podle shardu

**Zadání:** Zobrazte existující shardy, shardované kolekce, použité shard key a počet chunků pro jednotlivé kombinace kolekce a shardu.

**Řešení v MongoDB:**

```js
sh.status();
```

```js
use config;

db.collections.find(
  { _id: { $regex: "^projectdb\\." } },
  {
    _id: 1,
    key: 1,
    unique: 1
  }
);
```

```js
use config;

db.collections.aggregate([
  {
    $match: {
      _id: { $regex: "^projectdb\\." }
    }
  },
  {
    $lookup: {
      from: "chunks",
      localField: "uuid",
      foreignField: "uuid",
      as: "chunks"
    }
  },
  {
    $unwind: "$chunks"
  },
  {
    $group: {
      _id: {
        namespace: "$_id",
        shard: "$chunks.shard"
      },
      chunk_count: { $sum: 1 }
    }
  },
  {
    $sort: {
      "_id.namespace": 1,
      "_id.shard": 1
    }
  }
]);
```

**Poznámka:** Dotazy do databáze `config` ověřují, že kolekce projektu jsou skutečně shardované a že MongoDB ukládá metadata o jejich distribuci mezi shardy.

### 5.6 Simulace výpadku sekundárního uzlu a ověření dostupnosti čtení

**Zadání:** Ověřte chování clusteru při výpadku sekundárního uzlu v replica setu. Nejprve zjistěte stav replica setu, poté zastavte sekundární uzel, ověřte jeho nedostupnost a spusťte čtecí dotaz nad daty. Nakonec uzel znovu spusťte a zkontrolujte jeho návrat do stavu `SECONDARY`.

**Řešení v MongoDB a Dockeru:**

```js
rs.status();
```

```bash
docker stop mongo2
```

```js
rs.status();
```

```js
use projectdb;

db.claims.find(
  {
    insurance_provider: "Aetna",
    "claim.claim_billing_date": {
      $gte: ISODate("2025-02-01T00:00:00.000Z"),
      $lt: ISODate("2025-03-01T00:00:00.000Z")
    }
  }
)
.sort({ "claim.claim_billing_date": -1 })
.limit(10);
```

```bash
docker start mongo2
```

```js
rs.status();
```

**Poznámka:** Tento postup ověřuje, že výpadek sekundárního uzlu nezastaví čtení dat, protože replica set má stále dostupný primární uzel a další repliku. Po restartu se zastavený uzel opět připojí do replica setu jako sekundární uzel.
