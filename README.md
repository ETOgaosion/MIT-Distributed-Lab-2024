# MIT Distributed Lab(6.824 $\rightarrow$ 6.5840)

This repo is based on [MIT Spring 2024 6.5840 Course and Lab](https://pdos.csail.mit.edu/6.824/)

- [x] Lab1: Map Reduce (<ins>SS, 100% pass</ins>)
    - pass all tests with `-race`
    - use `commit` call to promise server/client exit moment right
    - workers scalable
- [x] Lab2: Key/Value Server (<ins>M, 100% pass</ins>)
    - tests are extremely strict(for codes and machine), check [this article](https://juejin.cn/post/7332852200937898035), it may help a lot if you are stuck in memory and time overhead
- [x] Lab3: Raft (<ins>H, 100% pass</ins>)
    - [x] Lab3A: Leader Election (<ins>M</ins>)
        - Locks are disturbing, but remember that main process does not wait for go routine, so it's free to use lock inside threads, while the outside function is within lock
        - If you do respect the frameworks, some lab requirements are different from years before, like avoiding the use of `time.Timer/time.Ticker`, and all loop process should be in `ticker` only
    - [x] **Lab3B: log (<ins>HH</ins>)**
        - Not so hard, but very complicated, you'd better understand the algorithm through running test, rather than refer to different versions of others' codes, they can be confusing
        - It cost me a lot of time to debug and understand the algorithm, corner cases
    - [x] Lab3C: persistence (<ins>SS</ins>)
        - `TestFigure83C`/`TestFigure8Unreliable3C` cannot pass within 30s timeout
    - [x] Lab3D: log compaction (<ins>H</ins>)
        - Don't be confused by conceptions, **Snapshot is just Log Compaction not persistence, we cannot recover states from Snapshots**
        - But debug is hard, you'd better not modify other files' implementation like `config.go`, here is some hints:
            - If you encounter `one([cmd]) failed to reach agreement` error, there are two possible reasons:
                - Your 3A implementation has bugs, no leader was successfully elected
                - it means that `applyCh` is not the one `config.go` set up, note that `applierSnap` check the `rafts[i]` before check `applyCh` outputs
            - `readPersist` must reload `lastApplied`/`commitIndex` from `lastIncludedIndexRd` or new added server would get error to find the `lastApplied + 1` index
            - We cannot compact the log actively by running a go routine, older tests not support such Snapshot mechanism, see the conception above
- [x] Lab4: Fault tolerance Key/Value Service (<ins>S/M, 100% pass</ins>)
    - [x] Lab4A: Key/value service without snapshots(<ins>M</ins>)
        - [Client Linear RPC Specification](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf), Page 67
        - [How to pass TestSpeed4A](https://github.com/niebayes/MIT-6.5840/tree/no_logging?tab=readme-ov-file#如何通过testspeed3a测试)
        - **We accelarate the leader finding process by add a `GetState` RPC call, I do not know why the framework code must try every server each time, cost too much extra time**
        - `TestSpeed4A` cannot pass, the requests are theoretically not satisfied with lab3 timing requirements
        - **Do NOT modify your implementation of Raft in lab3 easily**, it's likely that in multi-process environment, any small design changes can cause critical bugs which is hardly understandable. **So if you have confidence of your lab3 tests, do not modify your implementation**. If you **really** detect some lab3 bugs by retesting Raft directly, you can fix them, and **make sure your Raft works perfectly after each modification**
        - Use another RPC call `GetState` to recognize leader faster, no need for extra communication costs
    - [x] Lab4B: Key/value service with snapshots (<ins>S</ins>)
        - We should save our kv storage as well as client sequences into snapshots, and save raft states
        - We tested a corner case handled roughly when in Lab3: in `AppendEntries`, if the leader try to send append entries which has already been send to snapshots by clients (But server's `nextIndex` array do not receive that news), this is gonna happen: `args.PrevLogIndex < rf.getFirstLogIndex()`, we should return `false` with `FirstIndex` set to `-1`, let server retry later. This modification can pass all tests, but may not be as clear as other theories
        - `go test` can not handle well with goroutines, Iterations can be easily stuck
        - golang has a poor `unsafe.Sizeof`
- [x] Lab5: Sharded Key/Value Service (<ins>Coding: M/H, Debug: HH, 100% pass</ins>)
    - Recommend [this blog](https://www.cnblogs.com/pxlsdz/p/15685837.html)
    - [x] Lab5A: The Controller and Static Sharding (<ins>S/M, 100% pass</ins>)
        - **TC: $O(2n)$ SC: $O(1)$ Shard rebalance algorithm (Not like others' costly $O(n^2)$): Calculate `avg` and `remains` number of shards in gids first, Use 0 gid as a tmp storage for extra shards (larger than `avg + 1` if still got `remains`, larger than avg if `remains` is 0), then move all these shards to gid shards less than `avg`**
    - [x] **Lab5B (<ins>Coding: M/H, Debug: HHH, 100% pass</ins>)**
        - Idea Refernce Blog: [https://www.inlighting.org/archives/mit-6.824-notes](https://www.inlighting.org/archives/mit-6.824-notes)
        - Bug log:
            - Snapshot Test (3 Days):
                - `time.Millisecond` typo: `time.Microsecond`
                - Bug: stuck on `rf.log` encoding process
                    - Why: newly created server cannot sync log and do correct snapshots, so `rf.log` grow bigger
                    - Solition: 
                        - Seperate snapshot with main loop engine, do not let channel stuck the snapshot process
                        - Judge the snapshot index, if is 0, do not do snapshot
            - UnReliable Test (0.5 Days)
                - repeated ctrler requests return `ErrOK` typo `ErrWrongLeader` (Most Serious)
            - All (1 Day):
                - **livelock: help new leader apply old logs**
                - `handleOps` return directly when new `SequenceNum` $\leq$ old
            - Other optimizations
        - Performance: We use conservative time intervals, so our performance is much lower than else. We may improve this later.
- [x] Challenges
    - [x] `TestChallenge1Delete`: easy one, send delete shard requests after install shards
    - [x] `TestChallenge2Unaffected`: check shards before common operations, if their states are `Serving`, they can be used directly
    - [ ] `TestChallenge2Partial`
 
Evaluation Level (due to my own experience, regardless of official assessment):

- <ins>SS</ins>: Too Simple
- <ins>S</ins>: Simple
- <ins>M</ins>: Medium
- <ins>H</ins>: Hard
- <ins>HH</ins>: Too Hard
- <ins>HHH</ins>: Extrememly Hard

This repo clearifies some FAQ, but some answers need to be reconsider: [https://github.com/niebayes/MIT-6.5840](https://github.com/niebayes/MIT-6.5840), it uses modularized development, codes are not so referencable.

Good reference: [https://github.com/lantin16/MIT6.824-2020-Lab/tree/main](https://github.com/lantin16/MIT6.824-2020-Lab/tree/main)

## Version

Go: `go version go1.21.3 linux/amd64`
Project based on `git://g.csail.mit.edu/6.5840-golabs-2024` Commit: `e7aea3e613fdf9814580c97fd56c22c86a798c0e`