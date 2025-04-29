- Performance tests (vs other DBs?)
- Replace error type
- Handle WouldBlock IO error
- Key, Value slices type (maybe use Bytes?), Key limits
- Range iterator, reverse iterator?
- Invalidate cache when a node is released √
- Remove cache from FreeList?
- Create buckets?
- Compaction API?
- Test coverage
- Node checksum?
- Revisit panics
- Fuzzying
- Bug: panics with 'parent must have at least 2 children' when running twice:
    * insert 1M, delete 1M;
    * In tranverse_merge when all leaf nodes are empty, then parent branch node fails assertion when trying to merge remaining child node
    with few elements. Original implementation recursively calls
    `rebalance` on parent after merging every child.
    * One option would be to ignore `parent must have at least 2 children` invariant and let it get merged recursively.
- Bug: very slow when insert/delete 1K entries with commit per 1 entry;
- Bug: merge is sometimes not done correctly when single node is left in a branch node;