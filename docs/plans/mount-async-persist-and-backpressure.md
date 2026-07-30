# Mount: async persist pipeline and write backpressure

**Superseded**: folded into `docs/plans/bounded-memory-io-pipeline.md`,
once a chat discussion revealed `store` has the same underlying problem
(bounding memory across a source and target that can each be slow) -
this file's content now lives there (mount-specific mechanics under
"Mount-specific detail"), covering both commands instead of just this
one. Kept as a pointer rather than deleted so old links/searches for this
filename land somewhere useful.
