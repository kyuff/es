package es_test

type EventMock struct{ ID int }

func (EventMock) EventName() string {
	return "EventMock"
}

type UpgradedEventMock struct{ Number int }

func (UpgradedEventMock) EventName() string {
	return "UpgradedEventMock"
}
