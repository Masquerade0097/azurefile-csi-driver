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

	"k8s.io/kubernetes/pkg/cloudprovider/providers/azure"
	"k8s.io/kubernetes/pkg/util/mount"

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

var (
	managedDiskPathRE = regexp.MustCompile(`.*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/disks/(.+)`)
	unmanagedDiskPathRE = regexp.MustCompile(`http(?:.*)://(?:.*)/vhds/(.+)`)
)

type Driver struct {
	csicommon.CSIDriver
	cloud *azure.Cloud
	mounter *mount.SafeFormatAndMount
}

var (
	vendorVersion   = "0.0.1"
)

// Creates a NewCSIDriver object. Assumes vendor version is equal to driver version &
// does not support optional driver plugin info manifest field. Refer to CSI spec for more details.
func NewDriver(nodeID string) *Driver {
	if nodeID == "" {
		glog.Fatalln("NodeID missing")
		return nil
	}

	driver := Driver{}

	driver.Name = driverName
	driver.Version = vendorVersion
	driver.NodeID = nodeID

	return &driver
}

func (d *Driver) Run(endpoint string) {
	glog.Infof("Driver: %v ", driverName)
	glog.Infof("Version: %s", vendorVersion)

	cloud, err := GetCloudProvider()
	if err != nil {
		glog.Fatalln("failed to get Azure Cloud Provider")
	}
	d.cloud = cloud

	d.mounter = &mount.SafeFormatAndMount{
		Interface: mount.New(""),
		Exec:      mount.NewOsExec(),
	}

	d.AddControllerServiceCapabilities(
		[]csi.ControllerServiceCapability_RPC_Type{
			csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
			csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
			//csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
			//csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		})
	d.AddVolumeCapabilityAccessModes([]csi.VolumeCapability_AccessMode_Mode{csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER})

	s := csicommon.NewNonBlockingGRPCServer()
	// Driver d act as IdentityServer, ControllerServer and NodeServer
	s.Start(endpoint, d, d, d)
	s.Wait()
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

func getDiskName(diskURI string) (string, error) {
	diskPathRE := managedDiskPathRE
	if !isManagedDisk(diskURI) {
		diskPathRE = unmanagedDiskPathRE
	}

	matches := diskPathRE.FindStringSubmatch(diskURI)
	if len(matches) != 2 {
		return "", fmt.Errorf("could not get disk name from %s, correct format: %s", diskURI, diskPathRE)
	}
	return matches[1], nil
}