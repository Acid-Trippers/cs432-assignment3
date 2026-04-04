# Assignment 3: Logical Dashboard & Transactional Validation

**Course:** CS 432 – Databases (Course Project / Track 2)
**Instructor:** Dr. Yogesh K. Meena
**Deadline:** 6:00 PM, 5 April 2026
**Institution:** Indian Institute of Technology, Gandhinagar
**Semester:** II (2025–2026)

---

## 1. Project Objective

This assignment evaluates two key components built on top of the hybrid database system from Assignment 2:

- **Logical Dashboard Interface** — A user-facing interface that presents data in a clear, structured, and intuitive format. Must abstract away all underlying storage complexities and display information as cohesive logical entities. Users must not need to understand how or where data is stored.

- **Transactional Behaviour Validation** — Controlled experiments that verify the system maintains ACID (Atomicity, Consistency, Isolation, Durability) properties during data operations, even when multiple storage backends are involved.

### Core Technical Pipeline

Assignment 3 extends the A2 architecture with two additional layers:

- **Phase 1: Dashboard Rendering** — Show data from both databases via a dashboard
- **Phase 2: Logical Data Reconstruction** — Provide users with tools to query the data
- **Phase 3: ACID Behaviour Validation** — Design experiments demonstrating ACID properties within the hybrid storage system

---

## 2. Logical Dashboard Requirements

The dashboard must present data in a user-centric form, independent of underlying storage mechanisms.

**The dashboard must represent data strictly according to the logical schema.**

### Must Support:
- Viewing active session information
- Listing logical entities and their instances
- Displaying fields and corresponding values
- Presenting results of executed queries

### Constraint:
> The interface **must not** reveal backend-specific details such as SQL table names, MongoDB collections, indexing, or schema decisions.

Any implementation medium (web-based or CLI) may be used. Evaluation focuses on **correctness and clarity**, not UI complexity.

---

## 3. Logical Query Execution Monitoring

The dashboard must allow submission and observation of logical queries.

### Example Query Format:
```json
{
  "operation": "read",
  "fields": ["username", "comments"]
}
```

### For Each Query, the System Should:
1. Interpret the request using metadata
2. Generate and execute backend-specific queries
3. Merge results into a unified logical response

### The Dashboard Must Display:
- Query input
- Logical result
- Execution status

---

## 4. Transaction Coordination Layer

The system must implement a transaction coordination mechanism that ensures consistent execution across multiple backends.

### The System Should:
- Treat each operation as a single logical transaction
- Execute backend operations within their respective transactional guarantees
- Detect failures during execution
- Ensure all-or-nothing behaviour

**On failure, all partial updates must be rolled back to prevent inconsistent state.**

---

## 5. ACID Validation Experiments

The system must be evaluated through experiments validating the following four properties:

### Atomicity
Operations must either fully complete or fully rollback across all backends.

### Consistency
All transactions must preserve defined constraints and valid system states.

### Isolation
Concurrent transactions must not interfere with each other or produce inconsistent results.

### Durability
Committed data must persist reliably, even after system interruptions.

---

## 6. Deliverables

### Submission Must Include:
- A single report: `group_name_report.pdf`
- A short demonstration video

### Report Requirements:
- **First page must include:**
  - GitHub repository link
  - Video demonstration link
- Description of dashboard design and implementation
- Explanation of transaction coordination mechanism
- ACID validation experiments and observations
- Discussion of system limitations

---

## 7. Marking Criteria

| Criterion | Focus Area |
|---|---|
| Dashboard Implementation | Logical clarity and correctness |
| Transaction Coordination | Reliable multi-backend execution |
| ACID Validation | Quality of experimental validation |
| System Robustness | Failure and concurrency handling |
| Report Quality | Clarity and technical depth |

---

## 8. Conclusion

This assignment evaluates the system's ability to provide a clear logical interface while maintaining ACID-compliant transactional behaviour across multiple storage backends.