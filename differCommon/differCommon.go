package differCommon

import (
	"encoding/json"
	"fmt"
	xdcrBase "github.com/couchbase/goxdcr/base"
	xdcrParts "github.com/couchbase/goxdcr/base/filter"
	xdcrLog "github.com/couchbase/goxdcr/log"
	"github.com/couchbase/goxdcr/metadata"
	"github.com/couchbase/goxdcr/metadata_svc"
	"github.com/couchbase/goxdcr/service_def"
	service_def_mock "github.com/couchbase/goxdcr/service_def/mocks"
	"github.com/couchbase/goxdcr/service_impl"
	"github.com/couchbase/goxdcr/streamApiWatcher"
	xdcrUtils "github.com/couchbase/goxdcr/utils"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/mock"
	"io/ioutil"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"xdcrDiffer/base"
	"xdcrDiffer/utils"
)

type XdcrDependencies struct {
	Utils                   xdcrUtils.UtilsIface
	metadataSvc             service_def.MetadataSvc
	RemoteClusterSvc        service_def.RemoteClusterSvc
	ReplicationSpecSvc      service_def.ReplicationSpecSvc
	collectionsManifestsSvc service_def.CollectionsManifestSvc
	logger                  *xdcrLog.CommonLogger
	xdcrTopologySvc         service_def.XDCRCompTopologySvc
	SelfRef                 *metadata.RemoteClusterReference
	selfRefPopulated        uint32
	SpecifiedRef            *metadata.RemoteClusterReference
	SpecifiedSpec           *metadata.ReplicationSpecification
	Filter                  xdcrParts.Filter
	SrcCapabilities         metadata.Capability
	TgtCapabilities         metadata.Capability
	capabilityOnce          sync.Once
	srcBucketManifest       *metadata.CollectionsManifest
	tgtBucketManifest       *metadata.CollectionsManifest

	selfDefaultPoolInfo map[string]interface{}
	selfPoolsNodes      map[string]interface{}

	// If non-empty, just stream these collection IDs from each side's DCP
	SrcCollectionIds []uint32
	TgtCollectionIds []uint32
	// Logically there should only be 1-1 mapping, but make this flexible just in case
	SrcToTgtColIdsMap map[uint32][]uint32

	// For collections migration mode, each Filter should cause one or more target collection IDs
	colFilterToTgtColIdsMap map[string][]uint32
	// Each Filter string above is translated into a consistent ordered list below. The *index* of each Filter
	// string will then be used for the remainder of the differ protocol, and used to determine if a source mutation
	// has passed a certain Filter or not
	ColFilterOrderedKeys        []string
	colFilterOrderedTargetNs    []*xdcrBase.CollectionNamespace
	ColFilterOrderedTargetColId []uint32
}

func NewXdcrDependencies() (*XdcrDependencies, error) {
	deps := &XdcrDependencies{
		Utils:                   xdcrUtils.NewUtilities(),
		logger:                  xdcrLog.NewLogger("XdcrDiffTool", nil),
		SrcToTgtColIdsMap:       make(map[uint32][]uint32),
		colFilterToTgtColIdsMap: map[string][]uint32{},
	}

	deps.SelfRef, _ = metadata.NewRemoteClusterReference("", base.SelfReferenceName,
		viper.GetString(base.SourceUrlKey), viper.GetString(base.SourceUsernameKey), viper.GetString(base.SourcePasswordKey),
		"", false, "", nil, nil, nil, nil)

	var err error
	deps.metadataSvc, err = metadata_svc.NewMetaKVMetadataSvc(nil, deps.Utils, true /*readOnly*/)
	if err != nil {
		return nil, err
	}

	uiLogSvcMock := &service_def_mock.UILogSvc{}
	uiLogSvcMock.On("Write", mock.Anything).Run(func(args mock.Arguments) { fmt.Printf("%v", args.Get(0).(string)) }).Return(nil)
	xdcrTopologyMock := &service_def_mock.XDCRCompTopologySvc{}
	setupXdcrToplogyMock(xdcrTopologyMock, deps)
	resolverSvcMock := &service_def_mock.ResolverSvcIface{}
	checkpointSvcMock := &service_def_mock.CheckpointsService{}
	manifestsSvcMock := &service_def_mock.ManifestsService{}
	manifestsSvcMock.On("GetSourceManifests", mock.Anything).Return(nil, service_def.MetadataNotFoundErr)
	manifestsSvcMock.On("GetTargetManifests", mock.Anything).Return(nil, service_def.MetadataNotFoundErr)

	replicationSettingSvc := metadata_svc.NewReplicationSettingsSvc(deps.metadataSvc, nil, xdcrTopologyMock)

	deps.RemoteClusterSvc, err = metadata_svc.NewRemoteClusterService(uiLogSvcMock, deps.metadataSvc, xdcrTopologyMock,
		xdcrLog.DefaultLoggerContext, deps.Utils)
	if err != nil {
		return nil, err
	}

	deps.ReplicationSpecSvc, err = metadata_svc.NewReplicationSpecService(uiLogSvcMock, deps.RemoteClusterSvc,
		deps.metadataSvc, xdcrTopologyMock, resolverSvcMock, deps.logger.LoggerContext(), deps.Utils,
		replicationSettingSvc)
	if err != nil {
		return nil, err
	}

	err = deps.retrieveReplicationSpecInfo()
	if err != nil {
		return nil, err
	}

	err = deps.retrieveCapabilities()
	if err != nil {
		return nil, err
	}

	securitySvc := &service_def_mock.SecuritySvc{}
	setupSecuritySvcMock(securitySvc)
	err = setupMyKVNodes(xdcrTopologyMock, deps)
	if err != nil {
		return nil, err
	}

	bucketTopologySvc, err := service_impl.NewBucketTopologyService(xdcrTopologyMock, deps.RemoteClusterSvc,
		deps.Utils, xdcrBase.TopologyChangeCheckInterval, deps.logger.LoggerContext(),
		deps.ReplicationSpecSvc, xdcrBase.HealthCheckInterval, securitySvc, streamApiWatcher.GetStreamApiWatcher)

	deps.collectionsManifestsSvc, err = metadata_svc.NewCollectionsManifestService(deps.RemoteClusterSvc,
		deps.ReplicationSpecSvc, uiLogSvcMock, deps.logger.LoggerContext(), deps.Utils, checkpointSvcMock,
		xdcrTopologyMock, bucketTopologySvc, manifestsSvcMock)
	if err != nil {
		return nil, err
	}

	return deps, nil
}

func (deps *XdcrDependencies) retrieveCapabilities() error {
	ref, err := deps.RemoteClusterSvc.RemoteClusterByRefName(deps.SpecifiedRef.Name(), false)
	if err != nil {
		return fmt.Errorf("retrieveClusterCapabilities.RemoteClusterByRefName(%v) - %v", deps.SpecifiedRef.Name(), err)
	}

	deps.TgtCapabilities, err = deps.RemoteClusterSvc.GetCapability(ref)
	if err != nil {
		return fmt.Errorf("retrieveClusterCapabilities.GetCapability(%v) - %v", deps.SpecifiedRef.Name(), err)
	}
	return nil
}

func (deps *XdcrDependencies) Logger() *xdcrLog.CommonLogger {
	return deps.logger
}

func (deps *XdcrDependencies) retrieveReplicationSpecInfo() error {
	// CBAUTH has already been setup
	var err error
	deps.SpecifiedRef, err = deps.RemoteClusterSvc.RemoteClusterByRefName(viper.GetString(base.RemoteClusterNameKey), true /*refresh*/)
	if err != nil {
		for err != nil && err == metadata_svc.RefreshNotEnabledYet {
			deps.logger.Infof("Difftool hasn't finished reaching out to remote cluster. Sleeping 5 seconds and retrying...")
			time.Sleep(5 * time.Second)
			deps.SpecifiedRef, err = deps.RemoteClusterSvc.RemoteClusterByRefName(viper.GetString(base.RemoteClusterNameKey), true /*refresh*/)
		}
		if err != nil {
			deps.logger.Errorf("Error retrieving remote clusters: %v\n", err)
			return err
		}
	}

	if viper.GetBool(base.EnforceTLSKey) && !deps.SpecifiedRef.IsHttps() {
		err = fmt.Errorf("enforceTLS requires that the remote cluster reference %v to use Full-Encryption mode", deps.SpecifiedRef.Name())
		deps.logger.Errorf(err.Error())
		return err
	}

	if viper.GetString(base.TargetUsernameKey) != "" && viper.GetString(base.TargetUsernameKey) != deps.SpecifiedRef.UserName() && viper.GetString(base.TargetPasswordKey) != "" && viper.GetString(base.TargetPasswordKey) != deps.SpecifiedRef.Password() {
		err = fmt.Errorf("user-specified username and password is different from that of the credentials from reference %v", deps.SpecifiedRef.Name())
		deps.logger.Errorf(err.Error())
		return err
	}

	specMap, err := deps.ReplicationSpecSvc.AllReplicationSpecs()
	if err != nil {
		deps.logger.Errorf("Error retrieving specs: %v\n", err)
		return err
	}

	for _, spec := range specMap {
		if spec.SourceBucketName == viper.GetString(base.SourceBucketNameKey) && spec.TargetBucketName == viper.GetString(base.TargetBucketNameKey) && spec.TargetClusterUUID == deps.SpecifiedRef.Uuid() {
			deps.SpecifiedSpec = spec
			break
		}
	}

	if deps.SpecifiedSpec == nil {
		deps.logger.Warnf("Unable to find Replication Spec with source %v target %v, attempting to create a temporary one\n", viper.GetString(base.SourceBucketNameKey), viper.GetString(base.TargetBucketNameKey))
		// Create a dummy spec
		deps.SpecifiedSpec, err = metadata.NewReplicationSpecification(viper.GetString(base.SourceBucketNameKey), "" /*sourceBucketUUID*/, deps.SpecifiedRef.Uuid(), viper.GetString(base.TargetBucketNameKey), "" /*targetBucketUUID*/)
		if err != nil {
			deps.logger.Errorf(err.Error())
		}
		return err
	}

	deps.logger.Infof("Found Remote Cluster: %v and Replication Spec: %v\n", deps.SpecifiedRef.String(), deps.SpecifiedSpec.String())

	return deps.PopulateSelfRef()
}

func (deps *XdcrDependencies) PopulateSelfRef() error {
	deps.SelfRef.HttpsHostName_ = viper.GetString(base.SourceUrlKey)
	deps.SelfRef.UserName_ = viper.GetString(base.SourceUsernameKey)
	deps.SelfRef.Password_ = viper.GetString(base.SourcePasswordKey)
	deps.SelfRef.HttpAuthMech_ = xdcrBase.HttpAuthMechPlain

	// Only grab certificate if on a loopback device
	if deps.SpecifiedRef.IsHttps() && utils.IsURLLoopBack(viper.GetString(base.SourceUrlKey)) {
		cert, err := utils.GetCertificate(deps.Utils, viper.GetString(base.SourceUrlKey),
			viper.GetString(base.SourceUsernameKey), viper.GetString(base.SourcePasswordKey), xdcrBase.HttpAuthMechPlain)
		if err != nil {
			return err
		}

		internalHttpsHostname, _, err := deps.Utils.HttpsRemoteHostAddr(viper.GetString(base.SourceUrlKey), nil)
		if err != nil {
			return fmt.Errorf("unable to get httpsRemoteHostAddr: %v", err)
		}

		deps.SelfRef.Certificate_ = cert
		refHttpAuthMech, defaultPoolInfo, _, err := deps.Utils.GetSecuritySettingsAndDefaultPoolInfo(viper.GetString(base.SourceUrlKey),
			internalHttpsHostname, deps.SelfRef.UserName(), deps.SelfRef.Password(),
			deps.SelfRef.Certificates(), deps.SelfRef.ClientCertificate(), deps.SelfRef.ClientKey(),
			deps.SelfRef.IsHalfEncryption(), deps.logger)
		if err != nil {
			return fmt.Errorf("unable to get security settings: %v", err)
		}
		deps.SelfRef.SetHttpAuthMech(refHttpAuthMech)
		deps.selfDefaultPoolInfo = defaultPoolInfo

		if refHttpAuthMech == xdcrBase.HttpAuthMechHttps {
			// Need to get the secure port and attach it
			internalSSLPort, internalSSLPortErr, _, _ := deps.Utils.GetRemoteSSLPorts(viper.GetString(base.SourceUrlKey), deps.logger)
			if internalSSLPortErr == nil {
				sslHostString := xdcrBase.GetHostAddr(xdcrBase.GetHostName(viper.GetString(base.SourceUrlKey)), internalSSLPort)
				deps.SelfRef.SetHttpsHostName(sslHostString)
				deps.SelfRef.SetActiveHttpsHostName(sslHostString)
				deps.logger.Infof("Received SSL port to be %v and setting TLS hostname to %v", internalSSLPort, sslHostString)
			}
		}
	}

	poolsNodesPath := "/pools/nodes"
	err, _ := deps.Utils.QueryRestApi(viper.GetString(base.SourceUrlKey), poolsNodesPath, false, xdcrBase.MethodGet, "", nil, 0, &deps.selfPoolsNodes, nil)
	if err != nil {
		return fmt.Errorf("unable to get pools/nodes information: %v", err)
	}

	// Do this last
	atomic.StoreUint32(&deps.selfRefPopulated, 1)
	return nil
}

// This is needed whenever source and tgt clusters are >= 7.0
func (deps *XdcrDependencies) PopulateManifestsAndMappings() error {
	var err error
	deps.logger.Infof("Waiting 15 sec for manfiest service to initialize and then getting manifest for source Bucket %v target Bucket %v...\n", deps.SpecifiedSpec.SourceBucketName, deps.SpecifiedSpec.TargetBucketName)
	time.Sleep(15 * time.Second)

	deps.srcBucketManifest, deps.tgtBucketManifest, err = deps.collectionsManifestsSvc.GetLatestManifests(deps.SpecifiedSpec, false)
	if err != nil {
		deps.logger.Errorf("PopulateManifestsAndMappings() - %v\n", err)
		return err
	}

	deps.logger.Infof("Source manifest: %v", deps.srcBucketManifest)
	deps.logger.Infof("Target manifest: %v", deps.tgtBucketManifest)
	// Store the manifests in files
	err = deps.outputManifestsToFiles(err)
	if err != nil {
		return err
	}

	modes := deps.SpecifiedSpec.Settings.GetCollectionModes()
	rules := deps.SpecifiedSpec.Settings.GetCollectionsRoutingRules()
	if modes.IsMigrationOn() && rules.IsExplicitMigrationRule() {
		deps.logger.Infof("Replication spec is using special migration mapping")
	} else if modes.IsMigrationOn() {
		deps.logger.Infof("Replication spec is using migration mode")
	} else if modes.IsExplicitMapping() {
		deps.logger.Infof("Replication spec is using explicit mapping")
	} else {
		deps.logger.Infof("Replication spec is using implicit mapping")
	}
	err = deps.compileCollectionMapping()
	if err != nil {
		return err
	}

	// Once hardcoded compilation map has been generated, just stream these Collection IDs from DCP to minimize other noise
	deps.generateSrcAndTgtColIds()

	return nil
}

func (deps *XdcrDependencies) outputManifestsToFiles(err error) error {
	srcManJson, err := json.Marshal(deps.srcBucketManifest)
	if err != nil {
		deps.logger.Errorf("SrcManifestMarshal - %v\n", err)
		return err
	}

	tgtManJson, err := json.Marshal(deps.tgtBucketManifest)
	if err != nil {
		deps.logger.Errorf("TgtManifestMarshal - %v\n", err)
		return err
	}

	err = ioutil.WriteFile(utils.GetManifestFileName(viper.GetString(base.SourceFileDirKey)), srcManJson, 0644)
	if err != nil {
		deps.logger.Errorf("SrcManifestWrite - %v\n", err)
		return err
	}

	err = ioutil.WriteFile(utils.GetManifestFileName(viper.GetString(base.TargetFileDirKey)), tgtManJson, 0644)
	if err != nil {
		deps.logger.Errorf("TgtManifestWrite - %v\n", err)
		return err
	}
	return nil
}

func (deps *XdcrDependencies) compileCollectionMapping() error {
	pair := metadata.CollectionsManifestPair{
		Source: deps.srcBucketManifest,
		Target: deps.tgtBucketManifest,
	}
	namespaceMapping, err := metadata.NewCollectionNamespaceMappingFromRules(pair, deps.SpecifiedSpec.Settings.GetCollectionModes(), deps.SpecifiedSpec.Settings.GetCollectionsRoutingRules(), false, false)
	if err != nil {
		deps.logger.Errorf("NewCollectionNamespaceMappingFromRules err: %v", err)
		return err
	}

	modes := deps.SpecifiedSpec.Settings.GetCollectionModes()
	rules := deps.SpecifiedSpec.Settings.GetCollectionsRoutingRules()
	if modes.IsMigrationOn() && !rules.IsExplicitMigrationRule() {
		return deps.compileMigrationMapping(namespaceMapping)
	} else {
		deps.compileHardcodedColToColMapping(namespaceMapping)
	}
	return nil
}

func (deps *XdcrDependencies) compileMigrationMapping(nsMappings metadata.CollectionNamespaceMapping) error {
	for srcNs, tgtNsList := range nsMappings {
		if len(tgtNsList) > 1 {
			return fmt.Errorf("Migration rules with more than one target namespace is not supported")
		}
		for _, tgtNs := range tgtNsList {
			colId, err := deps.tgtBucketManifest.GetCollectionId(tgtNs.ScopeName, tgtNs.CollectionName)
			if err != nil {
				deps.logger.Errorf("Cannot find target namespace in manifest: %v", tgtNs.ToIndexString())
				continue
			}
			if _, exists := deps.colFilterToTgtColIdsMap[srcNs.String()]; !exists {
				deps.colFilterToTgtColIdsMap[srcNs.String()] = []uint32{colId}
			} else {
				deps.colFilterToTgtColIdsMap[srcNs.String()] = append(deps.colFilterToTgtColIdsMap[srcNs.String()], colId)
			}
			deps.colFilterOrderedTargetNs = append(deps.colFilterOrderedTargetNs, tgtNs)
		}
		deps.ColFilterOrderedKeys = append(deps.ColFilterOrderedKeys, srcNs.String())
	}

	deps.logger.Infof("Collections Migrations filters:\n")
	for i, filterStr := range deps.ColFilterOrderedKeys {
		deps.logger.Infof("%v : %v -> %v", i, filterStr, deps.colFilterOrderedTargetNs[i].ToIndexString())
	}

	// Ensure that the colIdMappings are handled accordingly
	for _, targetNs := range deps.colFilterOrderedTargetNs {
		targetColId, err := deps.tgtBucketManifest.GetCollectionId(targetNs.ScopeName, targetNs.CollectionName)
		if err != nil {
			return fmt.Errorf("cannot find collection %v from manifest %v", targetNs.ToIndexString(), deps.tgtBucketManifest.String())
		}
		deps.SrcToTgtColIdsMap[0] = append(deps.SrcToTgtColIdsMap[0], targetColId)
		deps.ColFilterOrderedTargetColId = append(deps.ColFilterOrderedTargetColId, targetColId)
	}
	return nil
}

func (deps *XdcrDependencies) compileHardcodedColToColMapping(namespaceMapping metadata.CollectionNamespaceMapping) {
	for srcNs, tgtNamespaces := range namespaceMapping {
		for _, tgtNs := range tgtNamespaces {
			scopeName := srcNs.GetCollectionNamespace().ScopeName
			collectionName := srcNs.GetCollectionNamespace().CollectionName
			tgtScopeName := tgtNs.ScopeName
			tgtCollectionName := tgtNs.CollectionName
			srcColId, srcErr := deps.srcBucketManifest.GetCollectionId(scopeName, collectionName)
			tgtColId, tgtErr := deps.tgtBucketManifest.GetCollectionId(tgtScopeName, tgtCollectionName)

			if srcErr != nil {
				deps.logger.Errorf("Cannot find %v - %v from source manifest %v\n", scopeName, collectionName, srcErr)
				continue
			}
			if tgtErr != nil {
				deps.logger.Errorf("Cannot find %v - %v from target manifest %v\n", scopeName, collectionName, tgtErr)
				continue
			}

			tgtList := []uint32{tgtColId}
			deps.SrcToTgtColIdsMap[srcColId] = tgtList
		}
	}

	deps.logger.Infof("Collection namespace mapping: %v idsMap: %v", namespaceMapping, deps.SrcToTgtColIdsMap)
}

func (deps *XdcrDependencies) generateSrcAndTgtColIds() {
	tgtColIdDedupMap := make(map[uint32]bool)

	modes := deps.SpecifiedSpec.Settings.GetCollectionModes()
	rules := deps.SpecifiedSpec.Settings.GetCollectionsRoutingRules()
	var migrationMode bool

	if modes.IsMigrationOn() && !rules.IsExplicitMigrationRule() {
		migrationMode = true
		for _, tgtColIds := range deps.colFilterToTgtColIdsMap {
			deps.populateDedupColIds(tgtColIds, tgtColIdDedupMap)
		}
	} else {
		for srcColId, tgtColIds := range deps.SrcToTgtColIdsMap {
			if !migrationMode {
				deps.SrcCollectionIds = append(deps.SrcCollectionIds, srcColId)
			}
			deps.populateDedupColIds(tgtColIds, tgtColIdDedupMap)
		}
	}

	if migrationMode {
		// Migration mode wise we only pull from the source collectionID
		deps.SrcCollectionIds = []uint32{xdcrBase.DefaultCollectionId}
	}
}

func (deps *XdcrDependencies) populateDedupColIds(tgtColIds []uint32, tgtColIdDedupMap map[uint32]bool) {
	for _, tgtColId := range tgtColIds {
		_, exists := tgtColIdDedupMap[tgtColId]
		if !exists {
			tgtColIdDedupMap[tgtColId] = true
			deps.TgtCollectionIds = append(deps.TgtCollectionIds)
		}
	}
}

func (deps *XdcrDependencies) RetrieveClustersCapabilities(legacyMode bool) error {
	if !legacyMode {
		ref, err := deps.RemoteClusterSvc.RemoteClusterByRefName(deps.SpecifiedRef.Name(), false)
		if err != nil {
			return fmt.Errorf("retrieveClusterCapabilities.RemoteClusterByRefName(%v) - %v", deps.SpecifiedRef.Name(), err)
		}

		deps.TgtCapabilities, err = deps.RemoteClusterSvc.GetCapability(ref)
		if err != nil {
			return fmt.Errorf("retrieveClusterCapabilities.GetCapability(%v) - %v", deps.SpecifiedRef.Name(), err)
		}
	}

	// Self capabilities
	if atomic.LoadUint32(&deps.selfRefPopulated) == 0 {
		return fmt.Errorf("SelfRef has not been populated\n")
	}
	connStr, err := deps.SelfRef.MyConnectionStr()
	if err != nil {
		return fmt.Errorf("retrieveClusterCapabilities.myConnStr(%v) - %v", deps.SelfRef.Name(), err)
	}
	defaultPoolInfo, err := deps.Utils.GetClusterInfo(connStr, xdcrBase.DefaultPoolPath, deps.SelfRef.UserName(),
		deps.SelfRef.Password(), deps.SelfRef.HttpAuthMech(), deps.SelfRef.Certificates(),
		deps.SelfRef.SANInCertificate(), deps.SelfRef.ClientCertificate(), deps.SelfRef.ClientKey(),
		deps.logger)
	if err != nil {
		return fmt.Errorf("retrieveClusterCapabilities.getClusterInfo(%v) - %v", deps.SelfRef.Name(), err)
	}

	err = deps.SrcCapabilities.LoadFromDefaultPoolInfo(defaultPoolInfo, deps.logger)
	if err != nil {
		return fmt.Errorf("retrieveClusterCapabilities.LoadFromDefaultPoolInfo(%v) - %v", defaultPoolInfo, err)
	}

	deps.logger.Infof("Source cluster supports collections: %v Target cluster supports collections: %v\n",
		deps.SrcCapabilities.HasCollectionSupport(), deps.TgtCapabilities.HasCollectionSupport())

	if deps.SrcCapabilities.HasCollectionSupport() || deps.TgtCapabilities.HasCollectionSupport() {
		err = deps.populateCollectionsPreReq()
		if err != nil {
			return err
		}
	}
	return nil
}

func (deps *XdcrDependencies) populateCollectionsPreReq() error {
	if deps.SrcCapabilities.HasCollectionSupport() && deps.TgtCapabilities.HasCollectionSupport() {
		// Both have collections support
		if err := deps.PopulateManifestsAndMappings(); err != nil {
			return err
		}
	} else if deps.SrcCapabilities.HasCollectionSupport() && !deps.TgtCapabilities.HasCollectionSupport() {
		// Source has collections but target does not - stream only default collection from the source
		deps.SrcCollectionIds = append(deps.SrcCollectionIds, 0)
	} else if !deps.SrcCapabilities.HasCollectionSupport() && deps.TgtCapabilities.HasCollectionSupport() {
		// Source does not have collections but target does - stream only default collection from the target
		deps.TgtCollectionIds = append(deps.TgtCollectionIds, 0)
	} else {
		// neither have collections - dont' do anything
	}
	return nil
}

func setupSecuritySvcMock(securitySvc *service_def_mock.SecuritySvc) {
	securitySvc.On("IsClusterEncryptionLevelStrict").Return(false)
}

// This may be re-set up once self-reference is populated
func setupXdcrToplogyMock(xdcrTopologyMock *service_def_mock.XDCRCompTopologySvc, dependencies *XdcrDependencies) {
	xdcrTopologyMock.On("IsMyClusterEnterprise").Return(true, nil)
	xdcrTopologyMock.On("IsKVNode").Return(true, nil)
	xdcrTopologyMock.On("IsMyClusterEncryptionLevelStrict").Return(false)
	setupTopologyMockCredentials(xdcrTopologyMock, dependencies)
	setupTopologyMockConnectionString(xdcrTopologyMock, dependencies)
}

func setupMyKVNodes(topologyMock *service_def_mock.XDCRCompTopologySvc, deps *XdcrDependencies) error {
	// As of XDCR v8, pools/nodes endpoint is gone so we need to do things the legacy way
	nodesInfo := deps.selfPoolsNodes
	if nodes, ok := nodesInfo[base.NodesKey]; !ok {
		return fmt.Errorf("%v is not found from pools/nodes output", base.NodesKey)
	} else if nodesList, ok := nodes.([]interface{}); !ok {
		return fmt.Errorf("nodesList is not an interface list")
	} else {
		var found bool
		for _, node := range nodesList {
			nodeInfoMap, ok := node.(map[string]interface{})
			if !ok {
				// should never get here
				return fmt.Errorf("node type is %v", reflect.TypeOf(node))
			}
			thisNode, ok := nodeInfoMap[xdcrBase.ThisNodeKey]
			if ok {
				thisNodeBool, ok := thisNode.(bool)
				if !ok {
					// should never get here
					return fmt.Errorf("thisNode is %v", reflect.TypeOf(thisNode))
				}
				if thisNodeBool {
					// found current node
					found = true
				}
			}
			if found {
				ports := nodeInfoMap[xdcrBase.PortsKey]
				portsMap := ports.(map[string]interface{})
				directPort := portsMap[xdcrBase.DirectPortKey]
				directPortFloat := directPort.(float64)
				memcachedPort := uint16(directPortFloat)

				hostAddr := nodeInfoMap[xdcrBase.HostNameKey]
				hostAddrStr := hostAddr.(string)

				hostName := xdcrBase.GetHostName(hostAddrStr)
				memcachedAddr := xdcrBase.GetHostAddr(hostName, memcachedPort)
				topologyMock.On("MyKVNodes").Return([]string{memcachedAddr}, nil)
				break
			}
		}
		if !found {
			return fmt.Errorf("Unable to set memcached port")
		}
	}
	return nil
}

func setupTopologyMockConnectionString(xdcrTopologyMock *service_def_mock.XDCRCompTopologySvc, diffTool *XdcrDependencies) {
	connFunc := func() string {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			connStr, _ := diffTool.SelfRef.MyConnectionStr()
			return connStr
		} else {
			return ""
		}
	}

	errFunc := func() error {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return nil
		} else {
			return fmt.Errorf("Not initialized yet")
		}
	}

	xdcrTopologyMock.On("MyConnectionStr").Return(connFunc, errFunc)
}

func setupTopologyMockCredentials(xdcrTopologyMock *service_def_mock.XDCRCompTopologySvc, diffTool *XdcrDependencies) {
	getUserName := func() string {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.UserName()
		} else {
			return ""
		}
	}
	getPw := func() string {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.Password()
		} else {
			return ""
		}
	}
	getAuthMech := func() xdcrBase.HttpAuthMech {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.HttpAuthMech()
		} else {
			return xdcrBase.HttpAuthMechPlain
		}
	}
	getCert := func() []byte {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.Certificates()
		} else {
			return nil
		}
	}
	getSanCert := func() bool {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.SANInCertificate()
		} else {
			return false
		}
	}
	getClientCert := func() []byte {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.ClientCertificate()
		} else {
			return nil
		}
	}
	getClientKey := func() []byte {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return diffTool.SelfRef.ClientKey()
		} else {
			return nil
		}
	}
	getErr := func() error {
		if atomic.LoadUint32(&diffTool.selfRefPopulated) == 1 {
			return nil
		} else {
			return fmt.Errorf("Not initialized yet")
		}
	}
	xdcrTopologyMock.On("MyCredentials").Return(getUserName, getPw, getAuthMech, getCert, getSanCert, getClientCert, getClientKey, getErr)
}
