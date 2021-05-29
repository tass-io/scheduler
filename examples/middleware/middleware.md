# Middleware Development

本文将以 demo middleware 为例，说明开发一个 middleware 的整个过程，注意，middleware 的初始化在目前的实现中晚于 event。

## middleware 简介

middleware 将会在执行 `executeRunFunction` 中在开始函数调用前执行相关逻辑。

middleware 必须实现 `middleware.Handler`，middleware 必须自己完成实例化，并以单体的形式对外暴露服务（单体的设计是因为目前没有找到需要每个函数执行创建单独上下文的必要性，均以参数传递即可）。

`Manager.middleware` 函数将以 middleware 的 order 从小到大依次进行执行

这里需要注意，我们是在每次函数需要被执行前创建了这个机制，如果开发者要从 http 进来开始的 middleware，我建议打开 gin 的文档进行查看，例如实现对参数 workflowName 和 flowName 的检查等。

## demo middleware 的实现

demo middleware 首先必须实现 `middleware.Handler`。

demo middleware 有两次机会进行注册，第一次机会即在`init()`函数中进行注册，只要开发者保证该函数能被调用，即可完成注册。
第二次机会在 `initial.Initial` 函数中完成注册的调用，这里主要是为了方便动态的调整 middleware 的使用，例如根据 flag 调整 order 等。

`Handler` 函数是处理的主流程，`err != nil` 将直接进行返回。
`Handler` 返回值的第二个参数将在 `err == nil` 的情况下决定 middleware 接下来的执行。如果是 `Abort` 的情况下，则第一参数发挥效力，将作为 Manager 对于函数执行的返回值，如果是 `Next`，则第一参数无效，并按序进入下一个中间件的执行流程中。

demo middleware 这里为了能正常运行，这里考虑的是直接返回，并且 register 的 order 为 0。

## 实验

运行`build.sh`以完成构建，之后运行`main -l -w "./config/workflow.yaml"`，通过启动的日志可以查询到 demo middleware 已经完成了初始化和注册，同时可以看到 LSDS 这个中间件因为是 common middleware，所以他的注册是更靠后的。
```
demo register
register Demo with order 0
register LSDS with order 2
```
发起请求：
`curl --request POST "http://127.0.0.1:8080/v1/workflow/" --header 'Content-Type: application/json' --data-raw '{"workflowName": "simple", "flowName": "", "parameters": {}}'`

最终结果为：
```json
{
	"success": true,
	"message": "ok",
	"result": {
		"simple_mid": {
			"simple_branch_1": {
				"demo": "demo"
			},
			"simple_branch_2": {
				"demo": "demo"
			}
		}
	}
}
```

这里也可以再次强调，我们的每次调用是在每个函数内的，因此结果可能不是预期的 `{"demo", "demo"}`