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
	RegisterFileHandle(fileName string) (WriteFileCb, error)
	DeRegisterFileHandle(fileName string) error
}

type State int

const (
	Closed State = iota
	Open   State = iota
)

// Returns bytes written/appended, err
type WriteFileCb func([]byte) (int, error)

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

func NewFileDescriptorPool(maxFds uint64) *FdPool {
	pool := &FdPool{
		fdMap:        make(map[string]*internalFd),
		fdsInUseChan: make(chan *internalFd, maxFds),
		fdNeedsOpen:  make(chan bool),
	}
	return pool
}

func (fdp *FdPool) RegisterFileHandle(fileName string) (WriteFileCb, error) {
	fdp.mtx.Lock()
	defer fdp.mtx.Unlock()

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

	return ifd.Write, nil
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

func (fd *internalFd) Write(input []byte) (bytesWritten int, err error) {
	fd.mtx.Lock()
	defer fd.mtx.Unlock()

	if fd.state == Closed {
		// Try to put itself in the fds to be in use. If not successful, request a release then try again
		select {
		case *fd.requestOpenChan <- fd:
			// Got permission to open and stay open
			bytesWritten, err = fd.openAndWrite(input)
			if err != nil {
				// TODO - need to clean up go routine
				return
			}
		default:
			*fd.requestRelease <- true // This will notify and block until someone frees up
			*fd.requestOpenChan <- fd
			bytesWritten, err = fd.openAndWrite(input)
			if err != nil {
				// TODO - need to clean up go routine
				return
			}
		}
	} else {
		bytesWritten, err = fd.fileHandle.Write(input)
	}

	return
}

func (fd *internalFd) openAndWrite(input []byte) (int, error) {
	var err error
	fd.state = Open

	fd.fileHandle, err = os.OpenFile(fd.fileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return 0, err
	}

	fd.wg.Add(1)
	go fd.waitForClose()

	bytesWritten, err := fd.fileHandle.Write(input)
	return bytesWritten, err
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
		<-*fd.requestOpenChan
	case <-fd.exitChan:
		// Exit path
		// Free up one fd from the max queue
		<-*fd.requestOpenChan
	}
}
