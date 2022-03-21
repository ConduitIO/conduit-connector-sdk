// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/conduitio/conduit-connector-sdk (interfaces: Source)

package sdk

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockSource is a mock of Source interface.
type MockSource struct {
	ctrl     *gomock.Controller
	recorder *MockSourceMockRecorder
}

// MockSourceMockRecorder is the mock recorder for MockSource.
type MockSourceMockRecorder struct {
	mock *MockSource
}

// NewMockSource creates a new mock instance.
func NewMockSource(ctrl *gomock.Controller) *MockSource {
	mock := &MockSource{ctrl: ctrl}
	mock.recorder = &MockSourceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSource) EXPECT() *MockSourceMockRecorder {
	return m.recorder
}

// Ack mocks base method.
func (m *MockSource) Ack(arg0 context.Context, arg1 Position) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ack", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Ack indicates an expected call of Ack.
func (mr *MockSourceMockRecorder) Ack(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ack", reflect.TypeOf((*MockSource)(nil).Ack), arg0, arg1)
}

// Configure mocks base method.
func (m *MockSource) Configure(arg0 context.Context, arg1 map[string]string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Configure", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Configure indicates an expected call of Configure.
func (mr *MockSourceMockRecorder) Configure(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Configure", reflect.TypeOf((*MockSource)(nil).Configure), arg0, arg1)
}

// Open mocks base method.
func (m *MockSource) Open(arg0 context.Context, arg1 Position) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Open indicates an expected call of Open.
func (mr *MockSourceMockRecorder) Open(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockSource)(nil).Open), arg0, arg1)
}

// Read mocks base method.
func (m *MockSource) Read(arg0 context.Context) (Record, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0)
	ret0, _ := ret[0].(Record)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockSourceMockRecorder) Read(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockSource)(nil).Read), arg0)
}

// Teardown mocks base method.
func (m *MockSource) Teardown(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Teardown", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Teardown indicates an expected call of Teardown.
func (mr *MockSourceMockRecorder) Teardown(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Teardown", reflect.TypeOf((*MockSource)(nil).Teardown), arg0)
}

// mustEmbedUnimplementedSource mocks base method.
func (m *MockSource) mustEmbedUnimplementedSource() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedSource")
}

// mustEmbedUnimplementedSource indicates an expected call of mustEmbedUnimplementedSource.
func (mr *MockSourceMockRecorder) mustEmbedUnimplementedSource() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedSource", reflect.TypeOf((*MockSource)(nil).mustEmbedUnimplementedSource))
}
