# Validity & metadata

LEGEND uses a **validity-based metadata model** to describe which files are active
for a given category and time. This tutorial explains the structure of validity
entries and how to apply them in practice.

---

## Validity entry structure

Each validity entry contains the following fields:

- `valid_from`: timestamp indicating when the change becomes effective.
- `category`: metadata category, e.g., `"cal"`, `"phy"`, `"all"`. Here `"all"` means NOT does apply to all cetegories, but is its own category. 
- `mode`: operation type, one of `reset`, `append`, `remove`, or `replace`.
- `apply`: list of files affected by the operation.

Example:

```yaml
- valid_from: "20221118T000000Z"
  category: "cal"
  mode: "append"
  apply:
    - /data/l200/partition1/cal/fileA.h5
    - /data/l200/partition1/cal/fileB.h5
```

---

## Modes

- **reset**: replaces the entire active file list with the entries in `apply`.
- **append**: adds the listed files to the current active list.
- **remove**: deletes the listed files from the current active list.
- **replace**: substitutes one file for another.

---