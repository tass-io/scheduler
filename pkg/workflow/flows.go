package workflow

import (
	"errors"

	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/utils/common"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

const (
	RootCondition = "root"
)

var (
	ErrNoStartFound     = errors.New("no start found")
	ErrInvalidStatement = errors.New("statement is invalid")
	ErrFlowNotFound     = errors.New("flow not found")
)

// parallelConditions just handles Flow.Outputs and Condition.Flows, they do the same logic
func (m *Manager) parallelConditions(
	sp *span.Span, para map[string]interface{}, wf *serverlessv1alpha1.Workflow,
	target int, nexts []string) (map[string]interface{}, error) {

	if len(nexts) == 0 {
		return nil, nil
	}
	promises := []*CondPromise{}
	flow := wf.Spec.Spec[target]
	for _, next := range nexts {
		// think about all next is like a new workflow
		newSp := span.NewSpanFromSpanSibling(sp)
		cond := findConditionByName(next, &flow)
		p := NewCondPromise(m.executeCondition, next)
		zap.S().Debugw("call condition with parameter", "flow", newSp.GetFlowName(), "parameters", para, "target", target)
		newSp.Start(next)
		p.Run(newSp, cond, wf, target, para)
		promises = append(promises, p)
	}

	finalResult := make(map[string]interface{}, len(promises))

	for _, p := range promises {
		resp, err := p.GetResult()
		zap.S().Debugw("get resp with function", "function", p.name, "resp", resp)
		if err != nil {
			zap.S().Errorw("get error from condition promise", "err", err)
			return nil, err
		}
		if resp != nil {
			finalResult[p.name] = resp
		}
	}
	return finalResult, nil
}

// parallelFlowsWithSpan executes Flows in parallel,
// it's called when a Flow finishes execution and goes to the next Flows in parallel
func (m *Manager) parallelFlowsWithSpan(sp *span.Span, para map[string]interface{},
	wf *serverlessv1alpha1.Workflow, nexts []int) (map[string]interface{}, error) {

	if len(nexts) == 0 {
		return nil, nil
	}
	promises := []*FlowPromise{}
	for _, next := range nexts {
		newSp := span.NewSpanFromSpanSibling(sp)
		newSp.SetFlowName(wf.Spec.Spec[next].Name)
		p := NewFlowPromise(m.executeSpec, newSp.GetFlowName())
		zap.S().Debugw("call function with parameter", "flow", newSp.GetFlowName(), "parameters", para)
		newSp.Start(newSp.GetFlowName())
		p.Run(para, wf, newSp)
		promises = append(promises, p)
	}

	finalResult := make(map[string]interface{}, len(promises))

	for _, p := range promises {
		resp, err := p.GetResult()
		zap.S().Debugw("get resp with function", "function", p.name, "resp", resp)
		if err != nil {
			zap.S().Errorw("get error from function promise", "err", err)
			return nil, err
		}
		if resp != nil {
			finalResult[p.name] = resp
		}
	}
	return finalResult, nil
}

// parallelFlows is an encapsulation of parallelFlowsWithSpan
func (m *Manager) parallelFlows(sp *span.Span, para map[string]interface{},
	wf *serverlessv1alpha1.Workflow, nexts []int) (map[string]interface{}, error) {

	return m.parallelFlowsWithSpan(sp, para, wf, nexts)
}

// executeSpec is the main function about workflow control, it handles the main flow path
func (m *Manager) executeSpec(sp *span.Span, parameters map[string]interface{},
	wf *serverlessv1alpha1.Workflow) (map[string]interface{}, error) {

	zap.S().Debugw("executeSpec start", "parameters", parameters)
	para, err := common.CopyMap(parameters)
	if err != nil {
		zap.S().Errorw("copy map error", "err", err, "para", para)
		return nil, err
	}

	var targetFlowIndex int
	if sp.GetFunctionName() == "" {
		targetFlowIndex, err = findFlowByName(wf, sp.GetFlowName())
		if err != nil {
			zap.S().Errorw("fill in function name error", "err", err)
			return nil, err
		}
		sp.SetFunctionName(wf.Spec.Spec[targetFlowIndex].Function)
	} else {
		targetFlowIndex, err = findFlowByName(wf, sp.GetFlowName())
		if err != nil {
			zap.S().Debugw("executeSpec findFlowByName error", "err", err, "span", sp)
			return nil, err
		}
	}

	// execute the function and get results
	// enter in rootspan if not from promise
	// FIXME: targetFlowIndex now is useless here
	result, err := m.executeRunFunction(sp, para, wf, targetFlowIndex)
	if err != nil {
		zap.S().Errorw("executeRunFunction error", "err", err)
		return nil, err
	}
	// find next Flows after the execution
	// pay attention !!! here may change result
	nexts, err := m.findNext(sp, &result, wf, targetFlowIndex) // has serveral next functions
	if err != nil {
		zap.S().Errorw("findNext error", "err", err)
		return nil, err
	}

	// if reach ends, stop the Workflow and returns
	if isEnd(&wf.Spec.Spec[targetFlowIndex]) {
		return result, nil
	}

	// enter the next Flows with flow level span
	finalResult, err := m.parallelFlowsWithSpan(sp, result, wf, nexts)
	if err != nil {
		return nil, err
	}
	if finalResult == nil {
		return result, nil
	}
	return finalResult, err
}

// executeRunFunction runs function without other workflow logic, middlewares are injected here.
func (m *Manager) executeRunFunction(sp *span.Span, parameters map[string]interface{},
	wf *serverlessv1alpha1.Workflow, index int) (map[string]interface{}, error) {

	middlewareSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	middlewareSpan.Start(middlewareSpan.GetFunctionName() + "-middleware")
	midResult, decision, err := m.middleware(middlewareSpan, parameters)
	middlewareSpan.Finish()
	if err != nil {
		sp.Finish()
		return nil, err
	}
	zap.S().Infow("get middleware result", "result", midResult)
	switch decision {
	case middleware.Abort:
		{
			return midResult, nil
		}
	case middleware.Next:
		{
			// now nothing to do
		}
	}
	functionSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
	functionSpan.Start(sp.GetFunctionName() + "Function")
	defer functionSpan.Finish()
	return m.runner.Run(sp, parameters)
}

// findStart finds the start flow name of a Workflow,
// it returns the flow name and its function name
func findStart(wf *serverlessv1alpha1.Workflow) (string, string, error) {
	for _, flow := range wf.Spec.Spec {
		if flow.Role == serverlessv1alpha1.Start {
			return flow.Name, flow.Function, nil
		}
	}
	return "", "", ErrNoStartFound
}

// isEnd returns wether a Flow is end,
// end is a strong rule, ignore Outputs
func isEnd(flow *serverlessv1alpha1.Flow) bool {
	return flow.Role == serverlessv1alpha1.End
}

// findFlowByName returns a Flow index by the input name
func findFlowByName(wf *serverlessv1alpha1.Workflow, name string) (int, error) {
	for i, flow := range wf.Spec.Spec {
		if flow.Name == name {
			return i, nil
		}
	}
	return -1, ErrFlowNotFound
}

// findNext executes switch logic and returns the next flows index
// TODO: Explanation
func (m *Manager) findNext(sp *span.Span,
	result *map[string]interface{}, wf *serverlessv1alpha1.Workflow, target int) ([]int, error) {

	now := wf.Spec.Spec[target]
	var err error
	switch now.Statement {
	case serverlessv1alpha1.Direct:
		{
			nexts := make([]int, 0, len(now.Outputs))
			for _, name := range now.Outputs {
				n, err := findFlowByName(wf, name)
				if err != nil {
					zap.S().Debugw("find next findFlowByName error", "err", err, "name", name)
					return nil, err
				}
				nexts = append(nexts, n)
			}
			sp.Finish() // direct finish here
			return nexts, nil
		}
	case serverlessv1alpha1.Switch:
		{
			sp.Finish()
			conditionSpan := span.NewSpanFromTheSameFlowSpanAsParent(sp)
			// conditionSpan.Start("conditions")
			zap.S().Debugw("in switch with conditions", now.Conditions)
			rootcond := findRootCondition(&now)
			*result, err = m.executeCondition(conditionSpan, rootcond, wf, target, *result)
			zap.S().Debugw("in switch with get new result", "result", result)
			if err != nil {
				return nil, err
			}
			// change the result I'm sorry for it, the logics is evil
			nexts := make([]int, 0, len(now.Outputs))
			for _, name := range now.Outputs {
				n, err := findFlowByName(wf, name)
				if err != nil {
					zap.S().Debugw("find next findFlowByName error", "err", err, "name", name)
					return nil, err
				}
				nexts = append(nexts, n)
			}
			return nexts, nil
		}
	default:
		{
			return []int{-1}, ErrInvalidStatement
		}
	}
}

// executeCondition handles all about a condition.
func (m *Manager) executeCondition(sp *span.Span, condition *serverlessv1alpha1.Condition,
	wf *serverlessv1alpha1.Workflow, target int,
	functionResult map[string]interface{}) (map[string]interface{}, error) {

	branchRes := executeConditionLogic(condition, functionResult)
	var next *serverlessv1alpha1.Next
	if branchRes {
		next = &condition.Destination.IsTrue
	} else {
		next = &condition.Destination.IsFalse
	}

	// parallel execute flows and conditions
	// FIXME: Now not parallel execution
	mergedResult := map[string]interface{}{}
	// execute condition.Conditions
	conditionsResult, err := m.parallelConditions(sp, functionResult, wf, target, next.Conditions)
	if err != nil {
		zap.S().Errorw("error at parallel conditions", "err", err)
	} else if conditionsResult != nil {
		mergedResult["conditions"] = conditionsResult
	}
	// execute condition.Flows
	flowsNum := []int{}
	for _, flowName := range next.Flows {
		i, err := findFlowByName(wf, flowName)
		if err != nil {
			return nil, err
		}
		flowsNum = append(flowsNum, i)
	}
	zap.S().Debugw("after conditions flows", "flows", flowsNum)
	flowsResult, err := m.parallelFlows(sp, functionResult, wf, flowsNum)
	if err != nil {
		zap.S().Errorw("error at parallel flow", "err", err)
	} else if flowsResult != nil {
		mergedResult["flows"] = flowsResult
	}
	return mergedResult, err
}

// executeConditionLogic reads condition from Workflow.spec.spec.[x].conditions and does the comparaion
// it first checks the type of the condition,
// then converts the target and comparision to the real values,
// last it does the `compare`
//
// For example, a Condition is defined as below:
//   name: root
//   type: int
//   operator: eq
//   target: $.a
//   comparision: "5"
// 	 destination: ...
//
// we can see taht the type is `int`, the comparision is converted to 5 as int,
// the target is converted to an "int" by reading the upstream response.
// Then it starts the "compare" and returns the result.
func executeConditionLogic(condition *serverlessv1alpha1.Condition, functionResult map[string]interface{}) bool {

	var leftValue interface{}
	var rightValue interface{}
	switch condition.Type {
	case serverlessv1alpha1.Bool:
		{
			leftValue = new(bool)
			rightValue = new(bool)
		}
	case serverlessv1alpha1.String:
		{
			leftValue = new(string)
			rightValue = new(string)
		}
	case serverlessv1alpha1.Int:
		{
			leftValue = new(int)
			rightValue = new(int)
		}
	}

	// get the real values
	common.GetValue(functionResult, condition.Target, leftValue)
	common.GetValue(functionResult, string(condition.Comparision), rightValue)

	zap.S().Debugw("condition logic get value", "left", leftValue, "right", rightValue)
	return compare(leftValue, rightValue, condition.Operator)
}

// compare compares different type values and ops
func compare(left interface{}, right interface{}, op serverlessv1alpha1.OperatorType) bool {
	switch left.(type) {
	case *int:
		{
			leftValue := left.(*int)
			rightValue := right.(*int)
			result := compareInt(*leftValue, *rightValue, op)
			zap.S().Debugw("get compareInt result at compare", "result", result)
			return result
		}
	case *string:
		{
			leftValue := left.(*string)
			rightValue := right.(*string)
			return compareString(*leftValue, *rightValue, op)
		}
	case *bool:
		{
			leftValue := left.(*bool)
			rightValue := right.(*bool)
			return compareBool(*leftValue, *rightValue, op)
		}
	default:
		zap.S().Panic(op)
	}
	return false
}

// compareInt is a helper function for int comparaion,
// it converts the OperatorType to real int comparaion
func compareInt(left int, right int, op serverlessv1alpha1.OperatorType) bool {
	zap.S().Debugw("compareInt", "left", left, "right", right, "op", op)
	switch op {
	case serverlessv1alpha1.Eq:
		{
			return left == right
		}
	case serverlessv1alpha1.Ne:
		{
			return left != right
		}
	case serverlessv1alpha1.Lt:
		{
			return left < right
		}
	case serverlessv1alpha1.Le:
		{
			return left <= right
		}
	case serverlessv1alpha1.Gt:
		{
			zap.S().Debugw("compare int result", "left", left, "right", right, "result", left > right)
			return left > right
		}
	case serverlessv1alpha1.Ge:
		{
			return left >= right
		}
	default:
		{
			zap.S().Warnw("invalid operator, return false instead", "op", op)
			return false
		}
	}
}

// compareString is a helper function for string comparaion,
// it converts the OperatorType to real string comparaion
func compareString(left string, right string, op serverlessv1alpha1.OperatorType) bool {
	switch op {
	case serverlessv1alpha1.Eq:
		{
			return left == right
		}
	case serverlessv1alpha1.Ne:
		{
			return left != right
		}
	case serverlessv1alpha1.Lt:
		{
			return left < right
		}
	case serverlessv1alpha1.Le:
		{
			return left <= right
		}
	case serverlessv1alpha1.Gt:
		{
			return left > right
		}
	case serverlessv1alpha1.Ge:
		{
			return left >= right
		}
	default:
		{
			zap.S().Warnw("invalid operator, return false instead", "op", op)
			return false
		}
	}
}

// compareBool is a helper function for bool comparaion,
// it converts the OperatorType to real bool comparaion
func compareBool(left bool, right bool, op serverlessv1alpha1.OperatorType) bool {
	switch op {
	case serverlessv1alpha1.Eq:
		{
			return left == right
		}
	case serverlessv1alpha1.Ne:
		{
			return left != right
		}
	default:
		{
			zap.S().Warnw("invalid operator, return false instead", "op", op)
			return false
		}
	}
}

// middleware executes all middleware actions and make a decision.
// It contais the following steps:
// 1. Order all the registered middlewares in a increasing order;
// 2. For each middleware, it does the middleware.Handle function;
// 3. If the decision is NEXT, it continues to iterate the middleware stream;
// 4. If the decision is ABORT, it breaks the middleware stream and returns the result directly.
func (m *Manager) middleware(sp *span.Span, body map[string]interface{}) (map[string]interface{}, middleware.Decision, error) {
	if m.orderedMiddlewares == nil {
		m.orderedMiddlewares = middleware.GetOrderedSources()
	}
	zap.S().Infow("get m.middlewareOrder", "orders", m.orderedMiddlewares)
	for _, source := range m.orderedMiddlewares {
		if mid, existed := m.middlewares[source]; !existed {
			zap.S().Warnw("middleware execute not found", "middleware", source)
			continue
		} else {
			zap.S().Infow("run middleware", "source", source)
			result, decision, err := mid.Handle(sp, body)
			if err != nil {
				zap.S().Errorw("middleware error", "middleware", mid.GetSource(), "err", err)
				return nil, middleware.Abort, err
			}
			zap.S().Infow("middleware result", "result", result, "decision", decision)
			switch decision {
			case middleware.Next:
				{
					// ignore result
					continue
				}
			case middleware.Abort:
				{
					return result, middleware.Abort, nil
				}
			}
		}
	}
	return nil, middleware.Next, nil
}

// findConditionByName returns the condition by the given name
func findConditionByName(name string, flow *serverlessv1alpha1.Flow) *serverlessv1alpha1.Condition {

	for _, condition := range flow.Conditions {
		if condition.Name == name {
			return condition
		}
	}
	return nil
}

// findRootCondition returns the root condition
// By default, the root condition name must be "root"
func findRootCondition(flow *serverlessv1alpha1.Flow) *serverlessv1alpha1.Condition {
	return findConditionByName(RootCondition, flow)
}
