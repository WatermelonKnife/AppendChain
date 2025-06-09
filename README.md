
## AppendChain 代码仓库



### 介绍
AppendChain包含三项优化，（1）类型分离，（2）大值分离，（3）排序MPT。

相关代码已经耦合到Go-Ethereum中，具体为vendor/github.com/ethereum/go-ethereum目录下。下面将逐个介绍。

####1. 类型分离

首先， 在ethdb包中，我们实现对rocksdb的支持。然后修改core/rawdb下的数据写入函数，对于每类数据写入独立列族。

#### 2. 大值分离

我们在ethdb/rocksdb中，实现是否开启大值分离参数项，然后在core/rawdb/database.go中，新建NewRocksDBDatabase2（）方法，写入大值分离的参数项。

#### 排序MPT

我们将排序MPT集成进整个Geth项目中，包含读写的各个流程。包括core/state/statedb.go, core/state/trie.go, core/state/node.go, core/state/trie_node.go,等文件,
包括整个MPT读取和编码的逻辑.具体包括以下函数:
MPT节点创建:core/state/statedb.go 的New2()函数
集成新键的编码提交: trie/database.go 的Commit2()函数
获取节点数据: trie/database.go 的node()函数

等  

#### 执行测试

> 系统软件环境: ubuntu server 20.04 LTS

1. 在执行测试前，需要同步以太坊数据。

具体需要下载并编译go-ethereum项目（我们使用的版本是Geth V1.9.25），并执行以下命令：

``` nohup ./geth1.9.25 --syncmode full --cache 4096 --datadir /mnt/sdb/gethdata --gcmode=archive --port 30303 --http --http.addr 127.0.0.1 --http.port 8545 >> geth.log 2>&1 &

```

待日志中显示同步一定量区块后，可以执行重放过程。我们同步600万区块大约花费了2月左右。


2. 编译RocksDB

我们使用的是rocksdb - 7.10.2

具体可以参考 https://blog.csdn.net/Waterees/article/details/120774083

3. 执行重放过程

main/testWriteOPS.go 包含整个重放过程,请你不要修改go.mod构建的依赖树,直接运行即可.

在这里你还需要修改数据库路径, 
56行 ancientDbPath 表示你同步的以太坊原始数据存放的leveldb数据库地址.

60行 newDbPath 表示你要新建的数据库地址.

101行,修改循环变量, 修改为你要重放的区块数量.