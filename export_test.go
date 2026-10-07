package async

import "os"

// SetNotify replaces the function that relays the shutdown signals to the
// task runners and returns a function that restores it.
func SetNotify(fn func(chan<- os.Signal, ...os.Signal)) (restore func()) {
	prev := notify
	notify = fn
	return func() { notify = prev }
}
