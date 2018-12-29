/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azuredisk

import (
	"fmt"
	"regexp"
	"strings"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/azure"

	"github.com/Azure/azure-sdk-for-go/services/compute/mgmt/2018-10-01/compute"
	"github.com/andyzhangx/azurefile-csi-driver/pkg/csi-common"
	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
)

const (
	driverName      = "disk.csi.azure.com"
	topologyKey     = "topology." + driverName + "/zone"
	accountName     = "accountname"
	seperator       = "#"
	fileMode        = "file_mode"
	dirMode         = "dir_mode"
	gid             = "gid"
	vers            = "vers"
	defaultFileMode = "0777"
	defaultDirMode  = "0777"
	defaultVers     = "3.0"
)

const (
	defaultStorageAccountType       = compute.StandardLRS
	defaultAzureDiskKind            = v1.AzureManagedDisk
	defaultAzureDataDiskCachingMode = v1.AzureDataDiskCachingNone
)

var (
	supportedCachingModes = sets.NewString(
		string(api.AzureDataDiskCachingNone),
		string(api.AzureDataDiskCachingReadOnly),
		string(api.AzureDataDiskCachingReadWrite))

	supportedDiskKinds = sets.NewString(
		string(api.AzureSharedBlobDisk),
		string(api.AzureDedicatedBlobDisk),
		string(api.AzureManagedDisk))

	lunPathRE = regexp.MustCompile(`/dev/disk/azure/scsi(?:.*)/lun(.+)`)
)

type azureFile struct {
	driver *csicommon.CSIDriver

	ids *identityServer
	ns  *nodeServer
	cs  *controllerServer

	cap   []*csi.VolumeCapability_AccessMode
	cscap []*csi.ControllerServiceCapability
}

type azureFileVolume struct {
	VolName string `json:"volName"`
	VolID   string `json:"volID"`
	VolSize int64  `json:"volSize"`
	VolPath string `json:"volPath"`
}

type azureFileSnapshot struct {
	Name      string              `json:"name"`
	Id        string              `json:"id"`
	VolID     string              `json:"volID"`
	Path      string              `json:"path"`
	CreateAt  int64               `json:"createAt"`
	SizeBytes int64               `json:"sizeBytes"`
	Status    *csi.SnapshotStatus `json:"status"`
}

var azureFileVolumes map[string]azureFileVolume
var azureFileVolumeSnapshots map[string]azureFileSnapshot

var (
	azureFileDriver *azureFile
	vendorVersion   = "0.0.1"
)

func init() {
	azureFileVolumes = map[string]azureFileVolume{}
	azureFileVolumeSnapshots = map[string]azureFileSnapshot{}
}

func GetAzureFileDriver() *azureFile {
	return &azureFile{}
}

func NewIdentityServer(d *csicommon.CSIDriver) *identityServer {
	return &identityServer{
		DefaultIdentityServer: csicommon.NewDefaultIdentityServer(d),
	}
}

func NewControllerServer(d *csicommon.CSIDriver, cloud *azure.Cloud) *controllerServer {
	return &controllerServer{
		DefaultControllerServer: csicommon.NewDefaultControllerServer(d),
		cloud:                   cloud,
	}
}

func NewNodeServer(d *csicommon.CSIDriver, cloud *azure.Cloud) *nodeServer {
	return &nodeServer{
		DefaultNodeServer: csicommon.NewDefaultNodeServer(d),
		cloud:             cloud,
	}
}

func (f *azureFile) Run(nodeID, endpoint string) {
	glog.Infof("Driver: %v ", driverName)
	glog.Infof("Version: %s", vendorVersion)

	cloud, err := GetCloudProvider()
	if err != nil {
		glog.Fatalln("failed to get Azure Cloud Provider")
	}

	// Initialize default library driver
	f.driver = csicommon.NewCSIDriver(driverName, vendorVersion, nodeID)
	if f.driver == nil {
		glog.Fatalln("Failed to initialize azuredisk CSI Driver.")
	}
	f.driver.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		})
	f.driver.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	// Create GRPC servers
	f.ids = NewIdentityServer(f.driver)
	f.ns = NewNodeServer(f.driver, cloud)
	f.cs = NewControllerServer(f.driver, cloud)

	s := csicommon.NewNonBlockingGRPCServer()
	s.Start(endpoint, f.ids, f.cs, f.ns)
	s.Wait()
}

func getVolumeByID(volumeID string) (azureFileVolume, error) {
	if azureFileVol, ok := azureFileVolumes[volumeID]; ok {
		return azureFileVol, nil
	}
	return azureFileVolume{}, fmt.Errorf("volume id %s does not exit in the volumes list", volumeID)
}

func getVolumeByName(volName string) (azureFileVolume, error) {
	for _, azureFileVol := range azureFileVolumes {
		if azureFileVol.VolName == volName {
			return azureFileVol, nil
		}
	}
	return azureFileVolume{}, fmt.Errorf("volume name %s does not exit in the volumes list", volName)
}

func getSnapshotByName(name string) (azureFileSnapshot, error) {
	for _, snapshot := range azureFileVolumeSnapshots {
		if snapshot.Name == name {
			return snapshot, nil
		}
	}
	return azureFileSnapshot{}, fmt.Errorf("snapshot name %s does not exit in the snapshots list", name)
}

func getFileShareInfo(id string) (string, string, string, error) {
	segments := strings.Split(id, seperator)
	if len(segments) < 3 {
		return "", "", "", fmt.Errorf("error parsing volume id: %q, should at least contain two #", id)
	}
	return segments[0], segments[1], segments[2], nil
}

// check whether mountOptions contain file_mode, dir_mode, vers, gid, if not, append default mode
func appendDefaultMountOptions(mountOptions []string) []string {
	fileModeFlag := false
	dirModeFlag := false
	versFlag := false

	for _, mountOption := range mountOptions {
		if strings.HasPrefix(mountOption, fileMode) {
			fileModeFlag = true
		}
		if strings.HasPrefix(mountOption, dirMode) {
			dirModeFlag = true
		}
		if strings.HasPrefix(mountOption, vers) {
			versFlag = true
		}
	}

	allMountOptions := mountOptions
	if !fileModeFlag {
		allMountOptions = append(allMountOptions, fmt.Sprintf("%s=%s", fileMode, defaultFileMode))
	}

	if !dirModeFlag {
		allMountOptions = append(allMountOptions, fmt.Sprintf("%s=%s", dirMode, defaultDirMode))
	}

	if !versFlag {
		allMountOptions = append(allMountOptions, fmt.Sprintf("%s=%s", vers, defaultVers))
	}

	/* todo: looks like fsGroup is not included in CSI
	if !gidFlag && fsGroup != nil {
		allMountOptions = append(allMountOptions, fmt.Sprintf("%s=%d", gid, *fsGroup))
	}
	*/
	return allMountOptions
}

func getStorageAccount(secrets map[string]string) (string, string, error) {
	if secrets == nil {
		return "", "", fmt.Errorf("unexpected: getStorageAccount secrets is nil")
	}

	var accountName, accountKey string
	for k, v := range secrets {
		switch strings.ToLower(k) {
		case "accountname":
			accountName = v
		case "azurestorageaccountname": // for compatability with built-in azuredisk plugin
			accountName = v
		case "accountkey":
			accountKey = v
		case "azurestorageaccountkey": // for compatability with built-in azuredisk plugin
			accountKey = v
		}
	}

	if accountName == "" {
		return "", "", fmt.Errorf("could not find accountname or azurestorageaccountname field secrets(%v)", secrets)
	}
	if accountKey == "" {
		return "", "", fmt.Errorf("could not find accountkey or azurestorageaccountkey field in secrets(%v)", secrets)
	}

	return accountName, accountKey, nil
}

func isManagedDisk(diskURI string) bool{
	if len(diskURI) > 4 && strings.ToLower(diskURI[:4]) == "http" {
		return false
	}
	return true
}