# Review policy

- Every change lands through a merge request with a green pipeline (`make lint`, `make test`).
- The owner reviews and merges. An agent may prepare a change and review it, but never merges.
- A change is compared against the approved `plan.md` work unit: files outside the unit's approved
  set block the merge until the plan is updated and re-approved.
- Not waivable: a red pipeline, a skipped test, a committed secret or service account key, a
  billable cloud command without a matching `make down` path.
