参考美团开源的Leaf实现的go语言版leaf。非官方。

用法：
```text
1. go get github.com/zzonee/leaf-go
2. cd leaf-go/cmd && go build
3. ./cmd
```


特性：
1. 支持 基于数据库的双segment和基于snowflake算法的id分配
2. segment模式下，支持将segment缓存到文件，启动时优先从文件加载，减少段号浪费
3. segment模式下,可配置多DB，基于轮询的负载均衡
4. 使用简单，可以参考cmd/leaf.yaml进行配置，http请求路径可自定义
5. snowflake获取workerId、segment的数据库已预留接口，您可以方便地进行二次开发
6. 一个简单的http客户端

资料：

[Leaf——美团点评分布式ID生成系统](https://tech.meituan.com/2017/04/21/mt-leaf.html)

[Leaf项目 git 地址](https://github.com/Meituan-Dianping/Leaf)
