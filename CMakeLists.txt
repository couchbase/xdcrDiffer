PROJECT (xdcrDiffer)

GoModBuild(TARGET xdcrDiffer PACKAGE github.com/couchbase/xdcrDiffer
  INSTALL_PATH bin OUTPUT xdcrDiffer
  GOVERSION SUPPORTED_NEWER)

IF(NOT WIN32)
    INSTALL (PROGRAMS runDiffer.sh TYPE BIN)
ENDIF()
