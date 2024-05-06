
## 功能

#### kafka消费者
* 采用手动assign topic减少了与kafka coordinator交互过程（joinGroup和syncGroup），以及partition reblance过程中的consumer消费阻塞的问题

#### kafka生产者
* 类似撮合引擎，为了实现消息的顺序性和发送消息的可靠性，通常配置ack=-1，并采用同步发送消息+同步提交offset的可靠性，这极大的制约了发送的效率，导致框架处理效率低下
* 本代码优化还是采用了同步发送消息的方式，优化项包括： 批量消息发送（一次发送包括尽量多的撮合结果）、多线程并发（见下）、手动维护offset（减少了一次同步offset提交的block io）
* 本代码实现了：同步和异步发送 及 单挑和批量处理，可以根据业务需求通过配置文件指定需要的方式

#### 多线程
* 根据kafka partition多线程并发，业务层保证同一维度数据放入同一个partition，比如同一交易对；线程间独立
* 撮合逻辑是针对单一交易对的，同一个交易对放入同一partition后，在内存中维护交易对的orderBook完成撮合 
* 为了实现消息的顺序性，同一partition中交易对必须共用同一线程
* 撮合和发送到下游kafka同一线程，保证了生产消息的顺序性，以及dump过程中避免多线程并发导致发送到下游kafka的消息重复【撮合线程与kafka io thread是并发的】


#### DUMP
* 目前实现了5分钟定时（配置文件可配）进行dump，如果需要实现基于消息数量触发则在matching部分添加判断逻辑，调用dump函数即可
* 退出dump，向进程发送SIGTERM和SIGINT信号，会进行dump，目前dump实现方式采用的是FileChannel（同kafka实现）
* 定时Dump和退出Dump都是在撮合线程中并发执行的，每个交易对一个文件，方便进行查看和分析或者迁移处理节点
* 加载dump文件也是在撮合引擎中并发加载的，可以有效减少加载时间

#### TODO:
* 由于各家撮合引擎定义和实现方式的差异性，现有代码简单mock模拟了。 接下来计划集成exchange-core
* 实现方式：
1. 实现OrderBook接口、OrderBookFactory.newOrderBook生成自定义的实例
2. 委托单数据定义继承Event，并实现writeTo和readFrom接口功能，分别实现dump文件的写入和加载
3. 撮合结果继承EventResult，实现自有字段的定义和后续的处理
