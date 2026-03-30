# Rescue Raw Snapshots

This subtree holds the explicit upstream snapshots used by governed rescue-task
freezes.

Rules:

- each file must represent a historical snapshot with an explicit `snapshot_as_of`
  boundary
- each row must carry enough provenance to reconstruct the manual curation source
- only files referenced by a rescue freeze manifest or lineage artifact belong here
- post-cutoff adjudication snapshots must stay separate from pre-cutoff ranking inputs
