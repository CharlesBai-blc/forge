package job

import "testing"

func TestValidateWorkerTransition(t *testing.T) {
	allowed := map[[2]WorkerState]bool{
		{WorkerActive, WorkerCordoned}:   true,
		{WorkerActive, WorkerDraining}:   true,
		{WorkerActive, WorkerLost}:       true,
		{WorkerCordoned, WorkerActive}:   true,
		{WorkerCordoned, WorkerDraining}: true,
		{WorkerCordoned, WorkerLost}:     true,
		{WorkerCordoned, WorkerRemoved}:  true,
		{WorkerDraining, WorkerCordoned}: true,
		{WorkerDraining, WorkerLost}:     true,
		{WorkerLost, WorkerActive}:       true,
		{WorkerLost, WorkerCordoned}:     true,
		{WorkerLost, WorkerDraining}:     true,
		{WorkerLost, WorkerRemoved}:      true,
	}
	states := []WorkerState{
		WorkerActive, WorkerCordoned, WorkerDraining, WorkerLost, WorkerRemoved,
	}
	for _, from := range states {
		for _, to := range states {
			err := ValidateWorkerTransition(from, to)
			ok := allowed[[2]WorkerState{from, to}]
			if ok && err != nil {
				t.Errorf("%s -> %s: unexpected error: %v", from, to, err)
			}
			if !ok && err == nil {
				t.Errorf("%s -> %s: expected error", from, to)
			}
		}
	}
}
