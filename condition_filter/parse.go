package condition_filter

import (
	"errors"
	"strings"

	"github.com/golang/glog"
)

const (
	opSharp = iota
	opLeft
	opRight
	opOr
	opAnd
	opNot
)

const (
	outsidesCondition = iota
	inCondition
	inString
)

var errorParse = errors.New("parse condition error")

func parseBoolTree(c string) (node *OPNode, err error) {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("parse `%s` error at `%s`", c, r)
			node = nil
			err = errorParse
		}
	}()

	//glog.Info(c)
	c = strings.Trim(c, " ")
	if c == "" {
		return nil, nil
	}

	s2, err := buildRPNStack(c)
	if err != nil {
		return nil, err
	}
	//glog.Info(s2)
	s := make([]interface{}, 0)

	for _, e := range s2 {
		if c, ok := e.(Condition); ok {
			s = append(s, c)
		} else {
			sLen := len(s)
			op := e.(int)
			if op == opNot {
				right := s[sLen-1].(*OPNode)
				s = s[:sLen-1]
				node := &OPNode{
					op:    op,
					right: right,
				}
				s = append(s, node)
			} else {
				right := s[sLen-1].(*OPNode)
				left := s[sLen-2].(*OPNode)
				s = s[:sLen-2]
				node := &OPNode{
					op:    op,
					left:  left,
					right: right,
				}
				s = append(s, node)
			}
		}
	}

	//glog.Info(s)
	if len(s) != 1 {
		return nil, errorParse
	}
	return s[0].(*OPNode), nil
}

func buildRPNStack(c string) ([]interface{}, error) {
	var (
		state             = outsidesCondition
		i                 int
		length            = len(c)
		parenthesis       = 0
		conditionStartPos int

		s1 = []int{opSharp}
		s2 = make([]interface{}, 0)
	)

	// 哪些导致状态变化??

	for i < length {
		switch c[i] {
		case '(':
			switch state {
			case outsidesCondition: // push s1
				s1 = append(s1, opLeft)
			case inCondition:
				parenthesis++
			}
		case ')':
			switch state {
			case outsidesCondition:
				if !pushOp(opRight, &s1, &s2) {
					panic(c[:i+1])
				}

			case inCondition:
				parenthesis--
				if parenthesis == 0 {
					condition, err := NewSingleCondition(c[conditionStartPos : i+1])
					if err != nil {
						glog.Error(err)
						panic(c[:i+1])
					}
					n := &OPNode{
						condition: condition,
					}
					s2 = append(s2, n)
					state = outsidesCondition
				}
			}
		case '&':
			switch state {
			case outsidesCondition: // push s1
				if c[i+1] != '&' {
					panic(c[:i+1])
				} else {
					if !pushOp(opAnd, &s1, &s2) {
						panic(c[:i+1])
					}
					i++
				}
			}
		case '|':
			switch state {
			case outsidesCondition: // push s1
				if c[i+1] != '|' {
					panic(c[:i+1])
				} else {
					if !pushOp(opOr, &s1, &s2) {
						panic(c[:i+1])
					}
					i++
				}
			}
		case '!':
			switch state {
			case outsidesCondition: // push s1
				if n := c[i+1]; n == '|' || n == '&' || n == ' ' {
					panic(c[:i+1])
				}
				if !pushOp(opNot, &s1, &s2) {
					panic(c[:i+1])
				}
			}
		case '"':
			switch state {
			case outsidesCondition: // push s1
				panic(c[:i+1])
			case inString:
				state = inCondition
			}
		case ' ':
		default:
			if state == outsidesCondition {
				state = inCondition
				conditionStartPos = i
			}

		}
		i++
	}

	if state != outsidesCondition {
		return nil, errorParse
	}

	for j := len(s1) - 1; j > 0; j-- {
		s2 = append(s2, s1[j])
	}

	return s2, nil
}

func pushOp(op int, s1 *[]int, s2 *[]interface{}) bool {
	if op == opRight {
		return findLeftInS1(s1, s2)
	}
	return compareOpWithS1(op, s1, s2)
}

// find ( in s1
func findLeftInS1(s1 *[]int, s2 *[]interface{}) bool {
	var j int
	for j = len(*s1) - 1; j > 0 && (*s1)[j] != opLeft; j-- {
		*s2 = append(*s2, (*s1)[j])
	}

	if j == 0 {
		return false
	}

	*s1 = (*s1)[:j]
	return true
}

// compare op with ops in s1, and put them to s2
func compareOpWithS1(op int, s1 *[]int, s2 *[]interface{}) bool {
	var j int
	for j = len(*s1) - 1; j > 0; j-- {
		//if (*s1)[j] == _op_left || op > (*s1)[j] {
		n1 := (*s1)[j]
		b := true
		switch {
		case n1 == opLeft:
			break
		case op > n1:
			break
		case op == opNot && n1 == opNot:
			break
		default:
			b = false
		}
		if b {
			break
		}
		*s2 = append(*s2, n1)
	}

	*s1 = (*s1)[:j+1]
	*s1 = append(*s1, op)
	return true
}
