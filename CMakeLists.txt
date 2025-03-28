PROJECT (xdcrDiffer)

GoModBuild(TARGET xdcrDiffer PACKAGE github.com/couchbase/xdcrDiffer
  INSTALL_PATH bin OUTPUT xdcrDiffer
  GOVERSION SUPPORTED_NEWER)

IF(NOT WIN32)
  CONFIGURE_FILE(runDiffer.sh "${CMAKE_INSTALL_PREFIX}/bin/runDiffer.sh" @ONLY)
ENDIF()

GoModTidySetup()
