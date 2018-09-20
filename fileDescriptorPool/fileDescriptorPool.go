// Copyright (c) 2018 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package fileDescriptorPool

import (
	"fmt"
	"os"
	"sync"
)

type FdPoolIface interface {
	RegisterFileHandle(fileName string) (FileOp, FileOp, error) // Read, Write, err
	RegisterReadOnlyFileHandle(fileName string) (FileOp, error) // Read, err
	DeRegisterFileHandle(fileName string) error
}

type State int

const (
	Closed State = iota
	Open   State = iota
)

// Returns bytes written/appended/read, err
type FileOp func([]byte) (int, error)

type FdPool struct {
	mtx    sync.Mutex
	curFds uint64
	fdMap  map[string]*internalFd

	fdsInUseChan chan (*internalFd)
	fdNeedsOpen  chan bool
}

type internalFd struct {
	fileName   string
	fileHandle *os.File
	state      State
	mtx        sync.Mutex

	// Channels to file descriptor pool to request open or ask someone to give up their open fds
	requestOpenChan *(chan (*internalFd))
	requestRelease  *(chan bool)
	exitChan        chan bool

	wg sync.WaitGroup
}

func NewFileDescriptorPool(maxFds int) *FdPool {
	pool := &FdPool{
		fdMap:        make(map[string]*internalFd),
		fdsInUseChan: make(chan *internalFd, maxFds),
		fdNeedsOpen:  make(chan bool),
	}
	return pool
}

func (fdp *FdPool) RegisterFileHandle(fileName string) (FileOp, FileOp, error) {
	fdp.mtx.Lock()
	defer fdp.mtx.Unlock()

	ifd, err := fdp.registerInternalNoLock(fileName)
	if err != nil {
		return nil, nil, err
	}

	// Try to open so we can see if we hit the limit - if so sys will balk, no need for error return
	ifd.InitOpen(false /*readonly*/)

	return ifd.Read, ifd.Write, nil
}

func (fdp *FdPool) RegisterReadOnlyFileHandle(fileName string) (FileOp, error) {
	fdp.mtx.Lock()
	defer fdp.mtx.Unlock()

	ifd, err := fdp.registerInternalNoLock(fileName)
	if err != nil {
		return nil, err
	}

	// Try to open so we can see if we hit the limit - if so sys will balk, no need for error return
	ifd.InitOpen(true /*readonly*/)

	return ifd.Read, nil
}

func (fdp *FdPool) registerInternalNoLock(fileName string) (*internalFd, error) {
	if _, ok := fdp.fdMap[fileName]; ok {
		return nil, fmt.Errorf("FileName %v is already registered", fileName)
	}

	ifd := &internalFd{
		fileName:        fileName,
		state:           Closed,
		requestOpenChan: &(fdp.fdsInUseChan),
		requestRelease:  &(fdp.fdNeedsOpen),
		exitChan:        make(chan bool, 1),
	}
	fdp.fdMap[fileName] = ifd

	return ifd, nil
}

func (fdp *FdPool) DeRegisterFileHandle(fileName string) error {
	fdp.mtx.Lock()
	defer fdp.mtx.Unlock()
	var fd *internalFd
	var ok bool
	if fd, ok = fdp.fdMap[fileName]; !ok {
		return fmt.Errorf("FileName %v has not been registered", fileName)
	}
	fd.Close()
	return nil
}

func (fd *internalFd) InitOpen(readOnly bool) (err error) {
	fd.mtx.Lock()
	defer fd.mtx.Unlock()

	if fd.state == Closed {
		// Try to put itself in the fds to be in use. If not successful, request a release then try again
		select {
		case *fd.requestOpenChan <- fd:
			// Got permission to open and stay open
			err = fd.open(readOnly)
		default:
			err = fmt.Errorf("Not opened")
		}
	}
	return
}

func (fd *internalFd) readWriteOpInternal(input []byte, read bool) (bytes int, err error) {
	fd.mtx.Lock()
	defer fd.mtx.Unlock()

	if fd.state == Closed {
		// Try to put itself in the fds to be in use. If not successful, request a release then try again
		select {
		case *fd.requestOpenChan <- fd:
			// Got permission to open and stay open
			if read {
				bytes, err = fd.openAndRead(input)
			} else {
				bytes, err = fd.openAndWrite(input)
			}
			if err != nil {
				return
			}
		default:
			*fd.requestRelease <- true // This will notify and block until someone frees up
			*fd.requestOpenChan <- fd
			if read {
				bytes, err = fd.openAndRead(input)
			} else {
				bytes, err = fd.openAndWrite(input)
			}
			if err != nil {
				return
			}
		}
	} else {
		if read {
			bytes, err = fd.fileHandle.Read(input)
		} else {
			bytes, err = fd.fileHandle.Write(input)
		}
	}
	return
}

func (fd *internalFd) Read(input []byte) (int, error) {
	return fd.readWriteOpInternal(input, true /*read*/)
}

func (fd *internalFd) Write(input []byte) (bytesWritten int, err error) {
	return fd.readWriteOpInternal(input, false /*read*/)
}

func (fd *internalFd) open(readonly bool) (err error) {
	if readonly {
		fd.fileHandle, err = os.OpenFile(fd.fileName, os.O_RDONLY, 0444)
	} else {
		fd.fileHandle, err = os.OpenFile(fd.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	}
	if err != nil {
		return
	}
	fd.state = Open
	fd.wg.Add(1)
	go fd.waitForClose()
	return
}

// Mtx should be held
func (fd *internalFd) openAndWrite(input []byte) (int, error) {
	var err error
	err = fd.open(false /*readonly*/)
	if err != nil {
		return 0, err
	}

	fd.state = Open
	return fd.fileHandle.Write(input)
}

// Mtx should be held
func (fd *internalFd) openAndRead(requested []byte) (int, error) {
	var err error
	err = fd.open(true /*readonly*/)
	if err != nil {
		return 0, err
	}

	fd.state = Open
	return fd.fileHandle.Read(requested)
}

// External API only
func (fd *internalFd) Close() {
	fd.exitChan <- true
	fd.closeInternal()
	fd.wg.Wait()
}

func (fd *internalFd) closeInternal() {
	fd.mtx.Lock()
	defer fd.mtx.Unlock()
	if fd.state != Closed {
		fd.fileHandle.Close()
		fd.state = Closed
	}
}

func (fd *internalFd) waitForClose() {
	defer fd.wg.Done()
	select {
	case <-*fd.requestRelease:
		fd.closeInternal()
		// Free up one fd from the max queue
		select {
		case <-*fd.requestOpenChan:
		default:
		}
	case <-fd.exitChan:
		// Exit path
		// Free up one fd from the max queue, if possible
		select {
		case <-*fd.requestOpenChan:
		default:
		}
	}
}
