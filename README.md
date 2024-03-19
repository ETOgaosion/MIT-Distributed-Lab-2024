# MIT Distributed Lab(6.824 $\rightarrow$ 6.5840)

This repo is based on [MIT Spring 2024 Course and Lab](https://pdos.csail.mit.edu/6.824/)

- [x] Lab1: Map Reduce
    - pass all tests with `-race`
    - use `commit` call to promise server/client exit moment right
    - workers scalable
- [x] Lab2: Key/Value Server
    - tests are extremely strict(for codes and machine), check [this article](https://juejin.cn/post/7332852200937898035), it may help a lot if you are stuck in memory and time overhead
- [ ] Lab3: Raft
    - [x] Lab3A: Leader Election
        - Locks are disturbing, but remember that main process does not wait for go routine, so it's free to use lock inside threads, while the outside function is within lock
        - If you do respect the frameworks, some lab requirements are different from years before, like avoiding the use of `time.Timer/time.Ticker`, and all loop process should be in `ticker` only
    - [ ] Lab3B: log
    - [ ] Lab3C: persistence
    - [ ] Lab3D: log compaction