package job

import "testing"

// TestValidateTransition checks every pair among the six documented
// states. Missing entries, including self-transitions and hops out
// of succeeded or failed, must be rejected.
func TestValidateTransition(t *testing.T) {
	allowed := map[[2]JobState]bool{
		{JobQueued, JobAssigned}:   true,
		{JobQueued, JobFailed}:     true,
		{JobAssigned, JobRunning}:  true,
		{JobAssigned, JobQueued}:   true,
		{JobAssigned, JobLost}:     true,
		{JobRunning, JobSucceeded}: true,
		{JobRunning, JobFailed}:    true,
		{JobRunning, JobLost}:      true,
		{JobLost, JobQueued}:       true,
		{JobLost, JobFailed}:       true,
	}

	states := []JobState{
		JobQueued, JobAssigned, JobRunning, JobSucceeded, JobFailed, JobLost,
	}

	for _, from := range states {
		for _, to := range states {
			err := ValidateTransition(from, to)
			ok := allowed[[2]JobState{from, to}]
			if ok && err != nil {
				t.Errorf("%s -> %s: unexpected error: %v", from, to, err)
			}
			if !ok && err == nil {
				t.Errorf("%s -> %s: expected error", from, to)
			}
		}
	}
}

// TestValidateTransitionUnknown covers values that are not one of
// the six states, so they never appear in the pair loop above.
func TestValidateTransitionUnknown(t *testing.T) {
	if err := ValidateTransition(JobState("bogus"), JobQueued); err == nil {
		t.Fatal("unknown from: expected error")
	}
	if err := ValidateTransition(JobQueued, JobState("bogus")); err == nil {
		t.Fatal("unknown to: expected error")
	}
}
