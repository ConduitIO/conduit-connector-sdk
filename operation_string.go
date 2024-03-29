// Code generated by "stringer -type=Operation -linecomment"; DO NOT EDIT.

package sdk

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[OperationCreate-1]
	_ = x[OperationUpdate-2]
	_ = x[OperationDelete-3]
	_ = x[OperationSnapshot-4]
}

const _Operation_name = "createupdatedeletesnapshot"

var _Operation_index = [...]uint8{0, 6, 12, 18, 26}

func (i Operation) String() string {
	i -= 1
	if i < 0 || i >= Operation(len(_Operation_index)-1) {
		return "Operation(" + strconv.FormatInt(int64(i+1), 10) + ")"
	}
	return _Operation_name[_Operation_index[i]:_Operation_index[i+1]]
}
