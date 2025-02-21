package es_test

type EventMock struct{ ID int }

func (EventMock) Name() string {
	return "EventMock"
}

type UpgradedEventMock struct{ Number int }

func (UpgradedEventMock) Name() string {
	return "UpgradedEventMock"
}
