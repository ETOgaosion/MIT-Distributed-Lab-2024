# MIT Distributed Lab(6.824 $\rightarrow$ 6.5840)

This repo is based on [MIT Spring 2024 6.5840 Course and Lab](https://pdos.csail.mit.edu/6.824/)

- [x] Lab1: Map Reduce(<ins>SS</ins>)
    - pass all tests with `-race`
    - use `commit` call to promise server/client exit moment right
    - workers scalable
- [x] Lab2: Key/Value Server(<ins>M</ins>)
    - tests are extremely strict(for codes and machine), check [this article](https://juejin.cn/post/7332852200937898035), it may help a lot if you are stuck in memory and time overhead
- [x] Lab3: Raft(<ins>H</ins>)
    - [x] Lab3A: Leader Election(<ins>M</ins>)
        - Locks are disturbing, but remember that main process does not wait for go routine, so it's free to use lock inside threads, while the outside function is within lock
        - If you do respect the frameworks, some lab requirements are different from years before, like avoiding the use of `time.Timer/time.Ticker`, and all loop process should be in `ticker` only
    - [x] **Lab3B: log(<ins>HH</ins>)**
        - Not so hard, but very complicated, you'd better understand the algorithm through running test, rather than refer to different versions of others' codes, they can be confusing
        - It cost me a lot of time to debug and understand the algorithm, corner cases
    - [x] Lab3C: persistence(<ins>SS</ins>)
        - `TestFigure83C`/`TestFigure8Unreliable3C` cannot pass within 30s timeout
    - [x] Lab3D: log compaction(<ins>H</ins>)
        - Don't be confused by conceptions, **Snapshot is just Log Compaction not persistence, we cannot recover states from Snapshots**
        - But debug is hard, you'd better not modify other files' implementation like `config.go`, here is some hints:
            - If you encounter `one([cmd]) failed to reach agreement` error, there are two possible reasons:
                - Your 3A implementation has bugs, no leader was successfully elected
                - it means that `applyCh` is not the one `config.go` set up, note that `applierSnap` check the `rafts[i]` before check `applyCh` outputs
            - `readPersist` must reload `lastApplied`/`commitIndex` from `lastIncludedIndexRd` or new added server would get error to find the `lastApplied + 1` index
            - We cannot compact the log actively by running a go routine, older tests not support such Snapshot mechanism, see the conception above
- [ ] Lab4: Fault tolerance Key/Value Service
    - [x] Lab4A: Key/value service without snapshots(<ins>M</ins>)
        - [Client Linear RPC Specification](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf), Page 67
        - [How to pass TestSpeed4A](https://github.com/niebayes/MIT-6.5840/tree/no_logging?tab=readme-ov-file#如何通过testspeed3a测试)
        - `TestSpeed4A` cannot pass, the requests are theorically not satisfied with lab3 timing requirements
        - **Do NOT modify your implementation of Raft in lab3 easily**, it's likely that in multi-process environment, any small design changes can cause critical bugs which is hardly understandable. **So if you have confidence of your lab3 tests, do not modify your implementation**. If you **really** detect some lab3 bugs by retesting Raft directly, you can fix them, and **make sure your Raft works perfectly after each modification**
 
Evaluation Level (due to my own experience, regardless of official assessment):

- <ins>SS</ins>: Too Simple
- <ins>S</ins>: Simple
- <ins>M</ins>: Medium
- <ins>H</ins>: Hard
- <ins>HH</ins>: Too Hard

This repo clearifies some FAQ, but some answers need to be reconsider: [https://github.com/niebayes/MIT-6.5840](https://github.com/niebayes/MIT-6.5840), it uses modularized development, codes are not so referencable.

## Version

Go: `go version go1.21.3 linux/amd64`