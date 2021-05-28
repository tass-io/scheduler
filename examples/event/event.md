# Event Scheduler

本文将介绍 Event 流程的相关内容，从实现上而言 Event 确实不是必须为扩缩容工作，但是该系统主要研究扩缩容，因此我们主要围绕扩缩容展开详解。

## Event 简介

首先 Event 必须实现 `event.Handler`，其中的 Start 函数将会在 Manager 完成初始化之后被调用，目前 Event 的注册在 Middleware 注册之前，Event 可以控制注册顺序，只需要在 `initial.Initial` 中进行修改即可，与 Middleware 注册不同的是，Event 的注册多一个参数 `delete`，该参数主要用于 `Decide` 函数中，当一个 event 的意见被采纳之后，时候选择删除该 event ，例如对于冷启动的 event，属于一次性消费类型，可以删除，但是对于 qps 的事件，在其还没更新的时候，可以不进行删除，这个值的设定主要取决于该 event 是否是一个长期意见，需要持续考虑。

Event 主要强调异步操作和随时产生，异步操作在于 event 最终的产生效力是异步的，随时产生是在于可以在整个程序的每一个地方都能进行 event 的添加。

每一个 `Event.Handler` 都需要注册，与 middleware 类似，但是其 order 主要是为了在 SchedulerHandler 中用于最终决策。

SchedulerHandler 是关于扩缩容的实践的绝对下游，即如果新开发的 Handler 希望能够影响到扩缩容，则必须要最终产生 ScheduleEvent 给 SchedulerHandler。

SchedulerHandler `Decide` 函数将依据 order 得出最终结论，并调用 `FunctionScheduler.Refresh` 进行最终的落实。 SchedulerHandler 将为每个函数记录上一次的最终结论，依据 order 对不同来源的 event 进行 `ScheduleEvent.Merge` 操作。简而言之，order 更小的决策力更高。