// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk (interfaces: Destination)
//
// Generated by this command:
//
//	mockgen -destination=mock_destination_test.go -self_package=github.com/conduitio/conduit-connector-sdk -package=sdk -write_package_comment=false . Destination
package sdk

import (
	context "context"
	reflect "reflect"

	config "github.com/conduitio/conduit-commons/config"
	opencdc "github.com/conduitio/conduit-commons/opencdc"
	gomock "go.uber.org/mock/gomock"
)

// MockDestination is a mock of Destination interface.
type MockDestination struct {
	ctrl     *gomock.Controller
	recorder *MockDestinationMockRecorder
}

// MockDestinationMockRecorder is the mock recorder for MockDestination.
type MockDestinationMockRecorder struct {
	mock *MockDestination
}

// NewMockDestination creates a new mock instance.
func NewMockDestination(ctrl *gomock.Controller) *MockDestination {
	mock := &MockDestination{ctrl: ctrl}
	mock.recorder = &MockDestinationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDestination) EXPECT() *MockDestinationMockRecorder {
	return m.recorder
}

// Configure mocks base method.
func (m *MockDestination) Configure(arg0 context.Context, arg1 config.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Configure", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Configure indicates an expected call of Configure.
func (mr *MockDestinationMockRecorder) Configure(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Configure", reflect.TypeOf((*MockDestination)(nil).Configure), arg0, arg1)
}

// LifecycleOnCreated mocks base method.
func (m *MockDestination) LifecycleOnCreated(arg0 context.Context, arg1 config.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LifecycleOnCreated", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// LifecycleOnCreated indicates an expected call of LifecycleOnCreated.
func (mr *MockDestinationMockRecorder) LifecycleOnCreated(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LifecycleOnCreated", reflect.TypeOf((*MockDestination)(nil).LifecycleOnCreated), arg0, arg1)
}

// LifecycleOnDeleted mocks base method.
func (m *MockDestination) LifecycleOnDeleted(arg0 context.Context, arg1 config.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LifecycleOnDeleted", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// LifecycleOnDeleted indicates an expected call of LifecycleOnDeleted.
func (mr *MockDestinationMockRecorder) LifecycleOnDeleted(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LifecycleOnDeleted", reflect.TypeOf((*MockDestination)(nil).LifecycleOnDeleted), arg0, arg1)
}

// LifecycleOnUpdated mocks base method.
func (m *MockDestination) LifecycleOnUpdated(arg0 context.Context, arg1, arg2 config.Config) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LifecycleOnUpdated", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// LifecycleOnUpdated indicates an expected call of LifecycleOnUpdated.
func (mr *MockDestinationMockRecorder) LifecycleOnUpdated(arg0, arg1, arg2 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LifecycleOnUpdated", reflect.TypeOf((*MockDestination)(nil).LifecycleOnUpdated), arg0, arg1, arg2)
}

// Open mocks base method.
func (m *MockDestination) Open(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *MockDestinationMockRecorder) Open(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockDestination)(nil).Open), arg0)
}

// Parameters mocks base method.
func (m *MockDestination) Parameters() config.Parameters {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Parameters")
	ret0, _ := ret[0].(config.Parameters)
	return ret0
}

// Parameters indicates an expected call of Parameters.
func (mr *MockDestinationMockRecorder) Parameters() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Parameters", reflect.TypeOf((*MockDestination)(nil).Parameters))
}

// Teardown mocks base method.
func (m *MockDestination) Teardown(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Teardown", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Teardown indicates an expected call of Teardown.
func (mr *MockDestinationMockRecorder) Teardown(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Teardown", reflect.TypeOf((*MockDestination)(nil).Teardown), arg0)
}

// Write mocks base method.
func (m *MockDestination) Write(arg0 context.Context, arg1 []opencdc.Record) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Write indicates an expected call of Write.
func (mr *MockDestinationMockRecorder) Write(arg0, arg1 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockDestination)(nil).Write), arg0, arg1)
}

// mustEmbedUnimplementedDestination mocks base method.
func (m *MockDestination) mustEmbedUnimplementedDestination() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedDestination")
}

// mustEmbedUnimplementedDestination indicates an expected call of mustEmbedUnimplementedDestination.
func (mr *MockDestinationMockRecorder) mustEmbedUnimplementedDestination() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedDestination", reflect.TypeOf((*MockDestination)(nil).mustEmbedUnimplementedDestination))
}
