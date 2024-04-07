# MIT Distributed Lab(6.824 $\rightarrow$ 6.5840)

This repo is based on [MIT Spring 2024 Course and Lab](https://pdos.csail.mit.edu/6.824/)

- [x] Lab1: Map Reduce(*SS*)
    - pass all tests with `-race`
    - use `commit` call to promise server/client exit moment right
    - workers scalable
- [x] Lab2: Key/Value Server(*M*)
    - tests are extremely strict(for codes and machine), check [this article](https://juejin.cn/post/7332852200937898035), it may help a lot if you are stuck in memory and time overhead
- [x] Lab3: Raft(*H*)
    - [x] Lab3A: Leader Election(*M*)
        - Locks are disturbing, but remember that main process does not wait for go routine, so it's free to use lock inside threads, while the outside function is within lock
        - If you do respect the frameworks, some lab requirements are different from years before, like avoiding the use of `time.Timer/time.Ticker`, and all loop process should be in `ticker` only
    - [x] **Lab3B: log(*HH*)**
        - Not so hard, but very complicated, you'd better understand the algorithm through running test, rather than refer to different versions of others' codes, they can be confusing
        - It cost me a lot of time to debug and understand the algorithm, corner cases
    - [x] Lab3C: persistence(*SS*)
        - `TestFigure83C`/`TestFigure8Unreliable3C` cannot pass within 30s timeout
    - [x] Lab3D: log compaction(*H*)
        - Don't be confused by conceptions, **Snapshot is just Log Compaction not persistence, we cannot recover states from Snapshots**
        - But debug is hard, you'd better not modify other files' implementation like `config.go`, here is some hints:
            - If you encounter `one([cmd]) failed to reach agreement` error, there are two possible reasons:
                - Your 3A implementation has bugs, no leader was successfully elected
                - it means that applyCh is not the one `config.go` setted up, note that `applierSnap` check the `rafts[i]` before check `applyCh` outputs
            - `readPersist` must reload `lastApplied`/`commitIndex` from `lastIncludedIndexRd` or new added server would get error to find the `lastApplied + 1` index
            - We cannot compact the log actively by running a go routine, older tests not support such Snapshot mechanism, see the conception above


Evaluation Level:

- *SS*: Too Simple
- *S*: Simple
- *M*: Medium
- *H*: Hard
- *HH*: Too Hard