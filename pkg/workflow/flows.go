package workflow

import (
	"errors"
	"github.com/tass-io/scheduler/pkg/middleware"
	"github.com/tass-io/scheduler/pkg/span"
	"github.com/tass-io/scheduler/pkg/tools/common"
	serverlessv1alpha1 "github.com/tass-io/tass-operator/api/v1alpha1"
	"go.uber.org/zap"
)

const (
	RootCondition = "root"
)

var (
	NoStartFoundError     = errors.New("no start found")
	InvalidStatementError = errors.New("statement is invalid")
	FlowNotFoundError     = errors.New("flow not found")
)

// parallelFlowsWithSpan just handle Flow.Outputs and Condition.Flows, they are the same logic
func (m *Manager) parallelConditions(para map[string]interface{}, wf *serverlessv1alpha1.Workflow, target int, nexts []string) (map[string]interface{}, error) {
	if len(nexts) == 0 {
		return nil, nil
	}
	promises := []*CondPromise{}
	flow := wf.Spec.Spec[target]
	for _, next := range nexts {
		// think about all next is like a new workflow
		newSp := span.Span{
			WorkflowName: wf.Name,
			FlowName:     flow.Name,
		}
		cond := findConditionByName(next, &flow)
		p := NewCondPromise(m.executeCondition, next)
		zap.S().Debugw("call condition with parameter", "flow", newSp.FlowName, "parameters", para, "target", target)
		p.Run(cond, wf, target, para)
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

// parallelFlowsWithSpan just handle Flow.Outputs and Condition.Flows, they are the same logic
func (m *Manager) parallelFlowsWithSpan(para map[string]interface{}, wf *serverlessv1alpha1.Workflow, nexts []int, sp span.Span) (map[string]interface{}, error) {
	if len(nexts) == 0 {
		return nil, nil
	}
	promises := []*FlowPromise{}
	for _, next := range nexts {
		// think about all next is like a new workflow
		newSp := span.Span{
			WorkflowName: sp.WorkflowName,
			FlowName:     wf.Spec.Spec[next].Name,
		}
		p := NewFlowPromise(m.executeSpec, newSp.FlowName)
		zap.S().Debugw("call function with parameter", "flow", newSp.FlowName, "parameters", para)
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

func (m *Manager) parallelFlows(para map[string]interface{}, wf *serverlessv1alpha1.Workflow, nexts []int) (map[string]interface{}, error) {
	sp := span.Span{
		WorkflowName: wf.Name,
	}
	return m.parallelFlowsWithSpan(para, wf, nexts, sp)
}

// executeSpec is the main function about workflow control, it handle the main flow path
func (m *Manager) executeSpec(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, sp span.Span) (map[string]interface{}, error) {
	zap.S().Debugw("executeSpec start", "parameters", parameters)
	para, err := common.CopyMap(parameters)
	if err != nil {
		zap.S().Errorw("copy map error", "err", err, "para", para)
		return nil, err
	}
	target, err := findFlowByName(wf, sp.FlowName)
	if err != nil {
		zap.S().Debugw("executeSpec findFlowByName error", "err", err, "span", sp)
		return nil, err
	}
	result, err := m.executeRunFunction(para, wf, target)
	if err != nil {
		zap.S().Errorw("executeRunFunction error", "err", err)
		return nil, err
	}
	// pay attention !!! here may change result
	nexts, err := m.findNext(&result, wf, target) // has serveral next functions
	if err != nil {
		zap.S().Errorw("findNext error", "err", err)
		return nil, err
	}

	if isEnd(&wf.Spec.Spec[target]) {
		return result, nil
	}

	finalResult, err := m.parallelFlowsWithSpan(result, wf, nexts, sp)
	if err != nil {
		return nil, err
	}
	if finalResult == nil {
		return result, nil
	}
	return finalResult, err
}

// executeRunFunction Run function without other logic
// middleware will inject there.
func (m *Manager) executeRunFunction(parameters map[string]interface{}, wf *serverlessv1alpha1.Workflow, index int) (map[string]interface{}, error) {

	sp := span.Span{
		WorkflowName: wf.Name,
		FlowName:     wf.Spec.Spec[index].Function,
	}
	zap.S().Info("run middleware")
	midResult, err := m.middleware(parameters, &sp)
	if err != nil {
		return nil, err
	}
	zap.S().Infow("get middleware result", "result", midResult)
	if midResult != nil {
		return midResult, nil
	}
	return m.runner.Run(parameters, sp)
}

// find the start flow name of a Workflow
func findStart(wf *serverlessv1alpha1.Workflow) (string, error) {
	for _, flow := range wf.Spec.Spec {
		if flow.Role == serverlessv1alpha1.Start {
			return flow.Name, nil
		}
	}
	return "", NoStartFoundError
}

// end is a strong rule, ignore Outputs
func isEnd(flow *serverlessv1alpha1.Flow) bool {
	return flow.Role == serverlessv1alpha1.End
}

func findFlowByName(wf *serverlessv1alpha1.Workflow, name string) (int, error) {
	for i, flow := range wf.Spec.Spec {
		if flow.Name == name {
			return i, nil
		}
	}
	return -1, FlowNotFoundError
}

// execute switch logic and return the next flows index
func (m *Manager) findNext(result *map[string]interface{}, wf *serverlessv1alpha1.Workflow, target int) ([]int, error) {
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
			return nexts, nil
		}
	case serverlessv1alpha1.Switch:
		{
			zap.S().Debugw("in switch with conditions", now.Conditions)
			rootcond := findRootCondition(&now)
			*result, err = m.executeCondition(rootcond, wf, target, *result)
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
			return []int{-1}, InvalidStatementError
		}
	}
}

// execute conditions use m.executeCondition
func (m *Manager) executeConditions(conditions []*serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow, target int, functionResult map[string]interface{}) (map[string]interface{}, error) {
	para, _ := common.CopyMap(functionResult)
	if conditions == nil || len(conditions) == 0 {
		return para, nil
	}
	var err error
	for _, condition := range conditions {
		zap.S().Debugw("condition execute with parameters", "condition", condition, "para", functionResult)
		para, err = m.executeCondition(condition, wf, target, para)
		if err != nil {
			zap.S().Errorw("execute condition error", "err", err)
		}
	}
	zap.S().Debugw("executeConditions with return value", "value", para)
	return para, nil
}

// handle all about a condition
func (m *Manager) executeCondition(condition *serverlessv1alpha1.Condition, wf *serverlessv1alpha1.Workflow, target int, functionResult map[string]interface{}) (map[string]interface{}, error) {
	branch := executeConditionLogic(condition, functionResult)
	var next *serverlessv1alpha1.Next
	if branch {
		next = &condition.Destination.IsTrue
	} else {
		next = &condition.Destination.IsFalse
	}
	// parallel execute flows and conditions
	mergedResult := map[string]interface{}{}
	conditionsResult, err := m.parallelConditions(functionResult, wf, target, next.Conditions)
	if err != nil {
		zap.S().Errorw("error at parallel conditions", "err", err)
	} else if conditionsResult != nil {

		mergedResult["conditions"] = conditionsResult
	}
	// execute condition.Flows
	// attention !!! not change to var flowsNum []int
	flowsNum := []int{}
	for _, flowName := range next.Flows {
		i, err := findFlowByName(wf, flowName)
		if err != nil {
			return nil, err
		}
		flowsNum = append(flowsNum, i)
	}
	zap.S().Debugw("after conditions flows", "flows", flowsNum)
	flowsResult, err := m.parallelFlows(functionResult, wf, flowsNum)
	if err != nil {
		zap.S().Errorw("error at parallel flow", "err", err)
	} else if flowsResult != nil {
		mergedResult["flows"] = flowsResult
	}
	return mergedResult, err
}

// just run GetValue and compare logic
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

	common.GetValue(functionResult, condition.Target, leftValue)
	common.GetValue(functionResult, string(condition.Comparision), rightValue)

	zap.S().Debugw("condition logic get value", "left", leftValue, "right", rightValue)
	return compare(leftValue, rightValue, condition.Operator)
}

// compare different type values and ops
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
		panic(op)
	}
}

// help function for int compare
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

// help function for string compare
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

// help function for bool compare
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

// execute middleware action
func (m *Manager) middleware(body map[string]interface{}, sp *span.Span) (map[string]interface{}, error) {
	if m.middlewareOrder == nil {
		m.middlewareOrder = middleware.Orders()
	}
	zap.S().Infow("get m.middlewareOrder", "orders", m.middlewareOrder)
	for _, source := range m.middlewareOrder {
		if mid, existed := m.middlewares[source]; !existed {
			zap.S().Warnw("middleware execute not found", "middleware", source)
			continue
		} else {
			zap.S().Infow("run middleware", "source", source)
			result, decision, err := mid.Handle(body, sp)
			if err != nil {
				zap.S().Errorw("middleware error", "middleware", mid.GetSource(), "err", err)
				return nil, err
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
					return result, nil
				}
			}
		}
	}
	return nil, nil
}

func findConditionByName(name string, flow *serverlessv1alpha1.Flow) *serverlessv1alpha1.Condition {

	for _, condition := range flow.Conditions {
		if condition.Name == name {
			return condition
		}
	}
	return nil
}

func findRootCondition(flow *serverlessv1alpha1.Flow) *serverlessv1alpha1.Condition {
	return findConditionByName(RootCondition, flow)
}
