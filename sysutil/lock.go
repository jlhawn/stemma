// +build !windows

package sysutil

import (
	"os"

	"golang.org/x/sys/unix"
)

// Lock represents a file lock.
type Lock struct {
	fd       int
	blocking bool
}

// NewLock creates a new file lock using the given file.
func NewLock(f *os.File) *Lock {
	return &Lock{fd: int(f.Fd())}
}

// SetBlocking sets whether or not to wait when an incompatible lock is held
// by another process. Defaults to false.
func (l *Lock) SetBlocking(blocking bool) {
	l.blocking = blocking
}

// SharedLock acquires the file lock for shared use. If blocking is disabled
// and an incompatible lock is held by another process, the returned error
// will indicate EWOULDBLOCK.
func (l *Lock) SharedLock() error {
	return l.lock(unix.LOCK_SH)
}

// ExclusiveLock acquires the file lock for exclusive use. If blocking is
// disabled and an incompatible lock is held by another process, the returned
// error will indicate EWOULDBLOCK.
func (l *Lock) ExclusiveLock() error {
	return l.lock(unix.LOCK_EX)
}

// Unlock releases the file lock held by this process.
func (l *Lock) Unlock() error {
	return l.lock(unix.LOCK_UN)
}

// lock acquires or releases the file lock with the given option. If blocking
// is disabled, this method handles adding LOCK_NB to the option flags.
func (l *Lock) lock(opt int) error {
	if !l.blocking {
		opt |= unix.LOCK_NB
	}

	return unix.Flock(l.fd, opt)
}
