// Code generated by moq; DO NOT EDIT.
// github.com/matryer/moq

package hydrater_test

import (
	"context"
	"github.com/kyuff/es"
	"github.com/kyuff/es/hydrater"
	"sync"
)

// Ensure, that ProjectorMock does implement hydrater.Projector.
// If this is not the case, regenerate this file with moq.
var _ hydrater.Projector = &ProjectorMock{}

// ProjectorMock is a mock implementation of hydrater.Projector.
//
//	func TestSomethingThatUsesProjector(t *testing.T) {
//
//		// make and configure a mocked hydrater.Projector
//		mockedProjector := &ProjectorMock{
//			ProjectFunc: func(ctx context.Context, streamType string, streamID string, handler es.Handler) error {
//				panic("mock out the Project method")
//			},
//		}
//
//		// use mockedProjector in code that requires hydrater.Projector
//		// and then make assertions.
//
//	}
type ProjectorMock struct {
	// ProjectFunc mocks the Project method.
	ProjectFunc func(ctx context.Context, streamType string, streamID string, handler es.Handler) error

	// calls tracks calls to the methods.
	calls struct {
		// Project holds details about calls to the Project method.
		Project []struct {
			// Ctx is the ctx argument value.
			Ctx context.Context
			// StreamType is the streamType argument value.
			StreamType string
			// StreamID is the streamID argument value.
			StreamID string
			// Handler is the handler argument value.
			Handler es.Handler
		}
	}
	lockProject sync.RWMutex
}

// Project calls ProjectFunc.
func (mock *ProjectorMock) Project(ctx context.Context, streamType string, streamID string, handler es.Handler) error {
	if mock.ProjectFunc == nil {
		panic("ProjectorMock.ProjectFunc: method is nil but Projector.Project was just called")
	}
	callInfo := struct {
		Ctx        context.Context
		StreamType string
		StreamID   string
		Handler    es.Handler
	}{
		Ctx:        ctx,
		StreamType: streamType,
		StreamID:   streamID,
		Handler:    handler,
	}
	mock.lockProject.Lock()
	mock.calls.Project = append(mock.calls.Project, callInfo)
	mock.lockProject.Unlock()
	return mock.ProjectFunc(ctx, streamType, streamID, handler)
}

// ProjectCalls gets all the calls that were made to Project.
// Check the length with:
//
//	len(mockedProjector.ProjectCalls())
func (mock *ProjectorMock) ProjectCalls() []struct {
	Ctx        context.Context
	StreamType string
	StreamID   string
	Handler    es.Handler
} {
	var calls []struct {
		Ctx        context.Context
		StreamType string
		StreamID   string
		Handler    es.Handler
	}
	mock.lockProject.RLock()
	calls = mock.calls.Project
	mock.lockProject.RUnlock()
	return calls
}
