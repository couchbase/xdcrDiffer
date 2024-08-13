PROJECT (xdcrDiffer)

GoModBuild(TARGET xdcrDiffer PACKAGE github.com/couchbase/xdcrDiffer
  INSTALL_PATH bin OUTPUT xdcrDiffer
  GOVERSION SUPPORTED_NEWER)
